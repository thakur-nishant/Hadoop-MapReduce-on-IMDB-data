import matplotlib.pyplot as plt

fig, ax = plt.subplots()
filename = "mr_op.csv"
data = {}

with open(filename) as f:
    for line in f:
        raw_data = line[:-1].split(',')
        if int(raw_data[0][-4:]) in data:
            data[int(raw_data[0][-4:])].append([raw_data[1], raw_data[2]])
        else:
            data[int(raw_data[0][-4:])] = [[raw_data[1], raw_data[2]]]

print(data)

# top genre of each year
top_genre_per_year = [[],[]]

for year in data:
    x = data[year]
    temp = max(x, key= lambda x: x[1])
    top_genre_per_year[0].append(year)
    top_genre_per_year[1].append(temp[0])

ax.scatter(top_genre_per_year[0], top_genre_per_year[1])
ax.show()

number_of_action_year = [[], []]

x = []
y = []
for year in data:
    count = 0
    for genre in data[year]:
        if genre[0] == ' Adult ':
            count = genre[1]
    x.append(year)
    y.append(int(count))
    print(year, count)

print(x)
print(y)

# ax[0].hist(x)
# ax[1].hist(y)
plt.xlabel("year")
plt.ylabel("Number of Adult movies")
ax.plot(x, y,'.-')
plt.show()


