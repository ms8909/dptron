from lib.v2.Operations.readfile import ReadFile as read
from lib.v2.Middlewares.drop_col_with_null_val import DropNullValueCol

#### reading function
# res = read.read("./run/4alan_data_clean.csv", file_format='csv')
# res.show()

res = read.read("./run/4alan_data_clean.csv", file_format='csv')

drop_col = DropNullValueCol()
columns_to_drop = drop_col.delete_var_with_null_more_than(res, threshold=30)
print(columns_to_drop)
df = res.drop(*columns_to_drop)
