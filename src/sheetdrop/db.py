import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import select, desc
from sheetdrop.dbmodels import FileStatus, FileStatusDetail
from sheetdrop.enums import Status

def create_engine(url: str) -> Engine:
    """Create a database engine for use within FastApi
    Parameters:
        url: str
            The URL of the database
    Returns:
        sqlalchemy.engine.Engine
            The engine for the database
    """
    return sqlalchemy.create_engine(url)




def save_file_status(engine: Engine, file_id: str, status: Status, status_detail: list[str] = None) -> None:
    """Save the status of a file in the database
    Parameters:
        engine: sqlalchemy.engine.Engine
            The engine for the database
        file_id: str    
            The ID of the file
        status: Status 
            The status of the file
        status_detail: list[str]    
            The detail of the status
    """
    # Create a session
    with Session(engine) as session:
        # Create a new FileStatus entry
        new_status = FileStatus(file_id=file_id, status=status.value)
        # Add the status details to the new status
        for detail in status_detail or []:
            new_status_detail = FileStatusDetail(status_detail=detail)
            new_status.status_details.append(new_status_detail)
        # Add the new status (along with its details) to the session
        session.add(new_status)
        # Commit the transaction to save the new status and details
        session.commit()

def load_latest_file_status(engine: Engine, file_id: str) -> FileStatus:
    """Load the latest status of a file
    
    Parameters:
        engine: sqlalchemy.engine.Engine
            The engine for the database
        file_id: str
            The ID of the file
            
    Returns:
        FileStatus
            The latest status of the file, or None if no status is found
    """
    # Create a session
    with Session(engine) as session:
        # Query to get the latest FileStatus by file_id, ordered by timestamp descending
        stmt = select(FileStatus).where(FileStatus.file_id == file_id).order_by(desc(FileStatus.status_id))
        latest_status = session.scalars(stmt).first()
        # Get the status details from the latest status, to force eager loading
        details = latest_status.status_details if latest_status else None

        return latest_status
