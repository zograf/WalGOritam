import random, string, sys

file_name = "datoteka.txt"
# Number of records to generate
records = int(sys.argv[1])
with open(file_name, "w") as f:
    for _ in range(records):
        csv_code = ''.join(random.choices(string.ascii_letters, k=8))
        csv_id = random.randint(100, 1000)
        # csv_date = "28212021123321"
        # csv_cell = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 5))
        # csv_sentence = random.randint(0, 480)
        # csv_deleted = "False"
        f.write("PUT" + " " + csv_code + " " + str(csv_id) + "\n")
