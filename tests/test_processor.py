import csv
from pathlib import Path
import pytest
from worker.processor import process_csv


def test_csv_deduplication_bom_missing_and_idempotence(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    folder = tmp_path / 'tests' / 'nested'
    folder.mkdir(parents=True)
    (folder / 'input.csv').write_text('\ufeffa,b\n1,2\n1,2\n3,\n4\n', encoding='utf-8')
    result = process_csv('input.csv', 'job')
    assert {k: result[k] for k in ('total_rows', 'unique_rows', 'duplicate_rows', 'missing_values')} == dict(total_rows=4, unique_rows=3, duplicate_rows=1, missing_values=2)
    output = Path(result['output_file'])
    before = output.read_bytes()
    assert process_csv('input.csv', 'job') == result
    assert output.read_bytes() == before
    assert list(csv.reader(output.open())) == [['a', 'b'], ['1', '2'], ['3', ''], ['4']]
    assert len(list(output.parent.iterdir())) == 1


@pytest.mark.parametrize('content', [None, ''])
def test_missing_or_empty_csv(tmp_path, monkeypatch, content):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'tests').mkdir()
    if content is not None:
        (tmp_path / 'tests' / 'bad.csv').write_text(content)
    with pytest.raises(FileNotFoundError if content is None else ValueError):
        process_csv('bad.csv', 'job')
    assert not list(tmp_path.rglob('*.tmp'))


def test_atomic_replace_failure_preserves_output(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'tests').mkdir()
    source = tmp_path / 'tests' / 'x.csv'
    source.write_text('a\n1\n')
    output = Path(process_csv('x.csv', 'j')['output_file'])
    before = output.read_bytes()
    source.write_text('a\n2\n')
    def fail(*args):
        raise OSError('disk failure')
    monkeypatch.setattr('worker.processor.os.replace', fail)
    with pytest.raises(OSError):
        process_csv('x.csv', 'j')
    assert output.read_bytes() == before
    assert not list(output.parent.glob('*.tmp'))
