class Student:
    def __init__(self):
        self.raw_data = b''
        self.rows: int = 0
        self.offset = 0  # used later to know the offset from where the data of this table exist

student_table = Student()

