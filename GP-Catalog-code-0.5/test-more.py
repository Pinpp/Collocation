
from do_write import write_main
from do_read import read_main

data_lines_input = [['02/01/2022 03:25:17', 17.0, 29.0, 12.0, 'GP-PPT', 16.0, 'GP_67', 80.5376, -26.524, 0.308841717, 0.691494884, 0.652268514, -0.031581714, 'nan', 'FILTER', 'DEFAULT', 15.0,
                     300.0, 0.0, 1.0, 0.0, 0.0, 30742.0, 80.5376, 26.524, 154.4484, 126.0579, 124.0582, 90.0, 90.0, 143.9421, 126.1987, 125.8509, 90.3292, 89.6708, 143.7993, 'VT', 'HIGH', 'nan', 'nan', 'nan']]
catalog_id = write_main(data_lines_input)
print('Catalog ID:' + str(catalog_id))


print(read_main(catalog_id)[0])