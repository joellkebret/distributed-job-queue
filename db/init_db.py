# Creates the table if they don't exist
from db.database import engine
from db.models import Base

Base.metadata.create_all(bind=engine)