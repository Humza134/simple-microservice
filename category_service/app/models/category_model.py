from sqlmodel import SQLModel, Field, Column
from typing import List, Dict, Optional
from sqlalchemy.dialects.postgresql import JSON


# Category Table

class CategoryBase(SQLModel):
    name: str = Field(index=True, unique=True)
    
class Category(CategoryBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    products: Optional[List[Dict]] = Field(default=None, sa_column=Column(JSON))

class CategoryCreate(CategoryBase):
    pass


class CategoryUpdate(SQLModel):
    name: str | None = None

