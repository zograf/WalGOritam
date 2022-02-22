import random, string, sys

file_name = "datoteka.txt"
# Number of records to generate
records = int(sys.argv[1])
with open(file_name, "w") as f:
    generated_keys = []
    for _ in range(records):
        key = ''.join(random.choices(string.ascii_letters, k=8))
        generated_keys.append(key)
        value = random.randint(100, 1000)
        f.write("PUT" + " " + key + " " + str(value) + "\n")

    for _ in range(records // 2):
        key = generated_keys[random.randint(0, len(generated_keys) - 1)]
        toss = random.randint(0, 1)
        if toss == 1:
            f.write("DEL" + " " + key + "\n")
        else:
            f.write("GET" + " " + key + "\n")