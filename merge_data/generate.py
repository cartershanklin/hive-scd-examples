#!/usr/bin/python

import csv
import random
import sys
import time

total = int(sys.argv[1])
type = sys.argv[2]

from faker import Factory
faker = Factory.create()
writer = csv.writer(sys.stdout)

# Always the same names.
faker.seed(0)
contacts = []
for i in xrange(1, total+1):
	contacts.append([ i, faker.name(), faker.email(), faker.state_abbr() ])
if type == "create":
	for record in contacts:
		writer.writerow(record)

if type == "update":
	updates = int(sys.argv[3])
	new = int(sys.argv[4])
	faker.seed(time.time())
	for i in xrange(0, updates+1):
		j = random.randrange(1, total)
		record = contacts[j]
		if random.randrange(0, 10) >= 2:
			record[3] = faker.state_abbr()
		if random.randrange(0, 10) >= 4:
			record[2] = faker.email()
	for i in xrange(total+1, total+new+1):
		contacts.append([ i, faker.name(), faker.email(), faker.state_abbr() ])
	for record in contacts:
		writer.writerow(record)
