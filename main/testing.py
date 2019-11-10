from lib.v2.Operations.readfile import ReadFile as read
from lib.v2.Middlewares.drop_col_with_null_val import DropNullValueCol
from lib.v2.Middlewares.drop_col_with_same_val import DropSameValueColumn
res = read.read("./run/4alan_data_clean.csv", file_format='csv')

#### reading function
# res = read.read("./run/4alan_data_clean.csv", file_format='csv')
# res.show()

#### drop null value columns
# drop_col = DropNullValueCol()
# columns_to_drop = drop_col.delete_var_with_null_more_than(res, threshold=30)
# print(columns_to_drop)
# df = res.drop(*columns_to_drop)

#### drop same value columns
drop_same_val_col = DropSameValueColumn()
columns_to_drop = drop_same_val_col.delete_same_val_com(res)
print(columns_to_drop)
df = res.drop(*columns_to_drop)
