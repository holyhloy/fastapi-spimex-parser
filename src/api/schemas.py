import datetime

from pydantic import BaseModel, root_validator, model_validator


class ResultsSchema(BaseModel):
    oil_id: str = None
    delivery_type_id: str = None
    delivery_basis_id: str = None


class DynamicsResultsSchema(ResultsSchema):
    start_date: datetime.date
    end_date: datetime.date

    @model_validator(mode='after')
    def dates_range_validator(self):
        if self.start_date >= self.end_date:
            raise ValueError('Start date must be less than end date')
        return self