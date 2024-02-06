if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    # Specify your transformation logic here
    print(f"Preprocessing: row with zero passengers:{data['passenger_count'].isin([0]).sum()}")
    print(f"How many existing values of VendorID: {data['VendorID'].unique()}")
    data = data[data['passenger_count']>0]
    data = data[data['trip_distance']>0]
    data["lpep_pickup_date"] = data["lpep_pickup_datetime"].dt.date
    
    original_cols = data.columns
    data.columns  = (data.columns
                    .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                    .str.lower()
                    ) # The regular expression (?<=[a-z])(?=[A-Z]) is used to find the position where a lowercase letter is followed by an uppercase letter,
    num_cols_to_change=sum(original_cols !=data.columns)
    print(f'Number of columns need to be renamed as snake case: {num_cols_to_change}')

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output.columns.isin(["vendor_id"]).sum() >0
    assert output['passenger_count'].isin([0]).sum()==0
    assert output['trip_distance'].isin([0]).sum()==0
