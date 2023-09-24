class Student:
    def __init__(self):
        self.rows: int = 0
        self.offset = 0  # used later to know the offset from where the data of this table exist

    @property
    def raw_data(self):
        with open("dipen.db", "rb") as file:
            return file.read()


student_table = Student()
