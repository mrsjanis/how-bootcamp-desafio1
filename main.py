import os
import pathlib

from import_data import ImportData

importData = ImportData()

PATH = 'dataset'


def import_dataset():
    for dir in os.scandir(PATH):
        if dir.is_dir():
            for file in os.scandir(PATH + "\\" + dir.name):
                importData.read_file(file.name, file.path, dir.name, pathlib.Path(file.name).suffix)

    importData.close_conn()


if __name__ == '__main__':
    import_dataset()

