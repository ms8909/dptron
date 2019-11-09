from lib.v2.Operations.readfile import ReadFile as read


res = read.read("./run/4alan_data_clean.csv",file_format='csv')
res.show()