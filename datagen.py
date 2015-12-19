import sys, time
from faker import Factory
from uuid import uuid4


def get_header():
    return ["uuid VARCHAR", "first_name VARCHAR", "last_name VARCHAR", "address VARCHAR", "city VARCHAR",
            "state VARCHAR", "zipcode BIGINT", "birthday DATE", "favorite_color VARCHAR"]


if len(sys.argv) != 3:
    print "usage: python datagen.py <numrows> <outfile>"
    print '|'.join(get_header())
    sys.exit(1)

fake = Factory.create()
NUM_ROWS = int(sys.argv[1])
FILE = sys.argv[2]

def get_row():
    return [str(x) for x in
            [uuid4(), fake.first_name(), fake.last_name(), fake.street_address(), fake.city(), fake.state(),
             fake.zipcode(), int(time.mktime(fake.date_time().timetuple())), fake.safe_color_name()]]


if __name__ == "__main__":
    of = open(FILE, 'w') if FILE != '-' else sys.stdout
    for i in range(0, NUM_ROWS):
        of.write('|'.join(get_row()))
        of.write('\n')
