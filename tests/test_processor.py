import csv
from pathlib import Path
import pytest
from botocore.exceptions import ClientError
from worker.processor import process_csv


def test_csv_deduplication_bom_missing_and_idempotence(tmp_path, monkeypatch, s3_inputs):
    monkeypatch.chdir(tmp_path)
    s3_inputs.objects['input/input.csv'] = '\ufeffa,b\n1,2\n1,2\n3,\n4\n'.encode('utf-8')
    result = process_csv('input.csv', 'job')
    assert {k: result[k] for k in ('total_rows', 'unique_rows', 'duplicate_rows', 'missing_values')} == dict(total_rows=4, unique_rows=3, duplicate_rows=1, missing_values=2)
    output = Path(result['output_file'])
    before = output.read_bytes()
    assert process_csv('input.csv', 'job') == result
    assert output.read_bytes() == before
    assert result['output_s3_key'] == 'output/cleaned_job.csv'
    assert s3_inputs.objects[result['output_s3_key']] == before
    assert s3_inputs.uploads == ['output/cleaned_job.csv', 'output/cleaned_job.csv']
    assert list(csv.reader(output.open())) == [['a', 'b'], ['1', '2'], ['3', ''], ['4']]
    assert len(list(output.parent.iterdir())) == 1
    assert len(s3_inputs.downloads) == 2
    assert s3_inputs.downloads[0].parent != s3_inputs.downloads[1].parent
    assert all(not path.parent.exists() for path in s3_inputs.downloads)


@pytest.mark.parametrize('content', [None, ''])
def test_missing_or_empty_csv(tmp_path, monkeypatch, content, s3_inputs):
    monkeypatch.chdir(tmp_path)
    if content is not None:
        s3_inputs.objects['input/bad.csv'] = content.encode('utf-8')
    with pytest.raises(ClientError if content is None else ValueError):
        process_csv('bad.csv', 'job')
    assert not list(tmp_path.rglob('*.tmp'))
    assert all(not path.parent.exists() for path in s3_inputs.downloads)
    assert not s3_inputs.uploads


def test_atomic_replace_failure_preserves_output(tmp_path, monkeypatch, s3_inputs):
    monkeypatch.chdir(tmp_path)
    s3_inputs.objects['input/x.csv'] = b'a\n1\n'
    output = Path(process_csv('x.csv', 'j')['output_file'])
    before = output.read_bytes()
    s3_inputs.objects['input/x.csv'] = b'a\n2\n'
    def fail(*args):
        raise OSError('disk failure')
    monkeypatch.setattr('worker.processor.os.replace', fail)
    with pytest.raises(OSError):
        process_csv('x.csv', 'j')
    assert output.read_bytes() == before
    assert not list(output.parent.glob('*.tmp'))
    assert s3_inputs.objects['output/cleaned_j.csv'] == before
    assert len(s3_inputs.uploads) == 1


def test_interrupted_download_cleans_input_and_preserves_output(tmp_path, monkeypatch, s3_inputs):
    monkeypatch.chdir(tmp_path)
    s3_inputs.objects['input/x.csv'] = b'a\n1\n'
    output = Path(process_csv('x.csv', 'j')['output_file'])
    before = output.read_bytes()
    downloads = []

    def interrupted_download(bucket, key, destination):
        path = Path(destination)
        downloads.append(path)
        path.write_bytes(b'partial download')
        raise ConnectionError('S3 download interrupted')

    monkeypatch.setattr('worker.processor.s3_client.download_file', interrupted_download)
    with pytest.raises(ConnectionError, match='S3 download interrupted'):
        process_csv('x.csv', 'j')
    assert all(not path.parent.exists() for path in downloads)
    assert output.read_bytes() == before


def test_upload_failure_propagates_and_retry_uploads_same_job(tmp_path, monkeypatch, s3_inputs):
    monkeypatch.chdir(tmp_path)
    s3_inputs.objects['input/x.csv'] = b'a\n1\n1\n'

    def fail_upload(*args):
        raise ConnectionError('S3 upload interrupted')

    with monkeypatch.context() as patch:
        patch.setattr('worker.processor.s3_client.upload_file', fail_upload)
        with pytest.raises(ConnectionError, match='S3 upload interrupted'):
            process_csv('x.csv', 'j')

    assert 'output/cleaned_j.csv' not in s3_inputs.objects
    assert not list(tmp_path.rglob('*.tmp'))
    assert all(not path.parent.exists() for path in s3_inputs.downloads)
    result = process_csv('x.csv', 'j')
    assert s3_inputs.objects[result['output_s3_key']] == Path(result['output_file']).read_bytes()
    assert s3_inputs.uploads == ['output/cleaned_j.csv']
