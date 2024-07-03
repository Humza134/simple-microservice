from sqlmodel import SQLModel, Field, Column
from typing import Dict
from sqlalchemy.dialects.postgresql import JSON

class ProductBase(SQLModel):
    name: str
    description: str
    price: float
    expiry: str | None = None
    brand: str | None = None
    weight: float | None = None
    category_id: int # It shall be pre defined by Platform
    sku: str | None = None
    # rating: list["ProductRating"] = Relationship(back_populates="product")
    # image: str # Multiple | URL Not Media | One to Manu Relationship
    # quantity: int | None = None # Shall it be managed by Inventory Microservice
    # color: str | None = None # One to Manu Relationship
    # rating: float | None = None # One to Manu Relationship

class Product(ProductBase, table=True):
    id: int | None = Field(default=None, primary_key=True)
    category: Dict = Field(sa_column=Column(JSON))

class ProductCreate(ProductBase):
    pass








# class ProductRating(SQLModel, table=True):
#     id: int | None = Field(default=None, primary_key=True)
#     product_id: int = Field(foreign_key="product.id")
#     rating: int
#     review: str | None = None
#     product = Relationship(back_populates="rating")
    
    # user_id: int # One to Manu Relationship
    

class ProductUpdate(SQLModel):
    name: str | None = None
    description: str | None = None
    price: float | None = None
    expiry: str | None = None
    brand: str | None = None
    weight: float | None = None
    sku: str | None = None