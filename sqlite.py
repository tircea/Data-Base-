import sqlite3

tables = [
    {
        "tableName": "specialization",
        "fields": [
            {"name": "id", "params": "INTEGER PRIMARY KEY"},
            {"name": "name", "params": "TEXT NOT NULL"}
        ]
    },
    {
        "tableName": "client",
        "fields": [
            {"name": "id", "params": "INTEGER PRIMARY KEY"},
            {"name": "name", "params": "TEXT NOT NULL"},
            {"name": "telephone", "params": "INTEGER NOT NULL"},
            {"name": "email", "params": "TEXT NOT NULL UNIQUE"}
        ]
    },
    {
        "tableName": "employee",
        "fields": [
            {"name": "id", "params": "INTEGER PRIMARY KEY"},
            {"name": "name", "params": "TEXT NOT NULL"},
            {"name": "specialization", "params": "INTEGER NOT NULL REFERENCES specialization(id)"},
            {"name": "date_of_hiring", "params": "TEXT NOT NULL"},
            {"name": "telephone", "params": "INTEGER NOT NULL"}
        ]
    },
    {
        "tableName": "service",
        "fields": [
            {"name": "id", "params": "INTEGER PRIMARY KEY"},
            {"name": "name", "params": "TEXT NOT NULL"},
            {"name": "price", "params": "REAL NOT NULL"},
            {"name": "duration", "params": "TEXT NOT NULL"},
            {"name": "employee_id", "params": "INTEGER NOT NULL REFERENCES employee(id)"}
        ]
    },
    {
        "tableName": "appointment",
        "fields": [
            {"name": "id", "params": "INTEGER PRIMARY KEY"},
            {"name": "date", "params": "TEXT NOT NULL"},
            {"name": "employee_id", "params": "INTEGER NOT NULL REFERENCES employee(id)"},
            {"name": "client_id", "params": "INTEGER NOT NULL REFERENCES client(id)"},
            {"name": "service_id", "params": "INTEGER NOT NULL REFERENCES service(id)"}
        ]
    }
]


def executeSql(query, params = ()):
    global cursor
    global conn

    cursor.execute(query, params)
    conn.commit()
    return cursor


def createTable(tableName, fields):
    paramsFull = []
    for field in fields:
        fieldName = field["name"]
        fieldParams = field["params"]
        paramsFull.append(f"{fieldName} {fieldParams}")

    paramsFull = ", ".join(paramsFull)
    tableQuery = f"CREATE TABLE {tableName}({paramsFull});"
    return executeSql(tableQuery)


def table_exists(table_name):
    return fetch("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", (table_name,))[0] == 1

def fetch(query, params = (), is_many = False):
    cursor = executeSql(query, params)
    if is_many:
        return cursor.fetchall()
    else:
        return cursor.fetchone()

# Задание 1

conn = sqlite3.connect('database.db')
cursor = conn.cursor()

for table in tables:
    tableName = table["tableName"]
    fields =  table["fields"]
    if not table_exists(tableName):
        req = createTable(tableName, fields)
        print(f"Table {tableName} created!")


# Задание 2
executeSql("INSERT INTO specialization (name) VALUES (?)", ('Dermatology',))
executeSql("INSERT INTO specialization (name) VALUES (?)", ('Nail Master',))
executeSql("INSERT INTO specialization (name) VALUES (?)", ('Makeup Master',))


executeSql("INSERT INTO client (name, telephone, email) VALUES (?, ?, ?)", ('John Doe', 1234567890, 'johndoe@example.com'))
executeSql("INSERT INTO client (name, telephone, email) VALUES (?, ?, ?)", ('Jane Smith', 9876543210, 'janesmith@example.com'))


executeSql("INSERT INTO employee (name, specialization, date_of_hiring, telephone) VALUES (?, ?, ?, ?)", ('Dr. Alice', 1, '2022-01-10', 1122334455))
executeSql("INSERT INTO employee (name, specialization, date_of_hiring, telephone) VALUES (?, ?, ?, ?)", ('Dr. Bob', 2, '2019-05-15', 5566778899))

executeSql("INSERT INTO service (name, price, duration, employee_id) VALUES (?, ?, ?, ?)", ('Skin Consultation', 50.0, '01:00:00', 1))
executeSql("INSERT INTO service (name, price, duration, employee_id) VALUES (?, ?, ?, ?)", ('Makeup', 150.0, '01:30:00', 2))

executeSql("INSERT INTO appointment (date, employee_id, client_id, service_id) VALUES (?, ?, ?, ?)", ('2023-11-07 14:00:00', 1, 1, 1))
executeSql("INSERT INTO appointment (date, employee_id, client_id, service_id) VALUES (?, ?, ?, ?)", ('2023-11-08 10:30:00', 2, 2, 2))

# Задание 3
employes = fetch("SELECT employee.name, service.name FROM employee INNER JOIN service ON employee.id = service.employee_id WHERE service.price > 100;")
clients = fetch("SELECT client.name, appointment.date, service.name FROM appointment INNER JOIN client ON client.id = appointment.client_id INNER JOIN service ON service.id = appointment.service_id")

print("Перший запит: ")
print(employes)
print("Другий запит: ")
print(clients)

# Задание 4
executeSql("UPDATE client SET name = ?, telephone = ? WHERE name = ?", ('Peter Grifin', 1010101010, 'John Doe'))
# Задание 5
clients = fetch("SELECT client.name, appointment.date, service.name FROM appointment INNER JOIN client ON client.id = appointment.client_id INNER JOIN service ON service.id = appointment.service_id")
print("Той запит після оновлення данних: ")
print(clients)

for table in tables:
    talbeName = table["tableName"]
    executeSql(f"DELETE FROM {talbeName}") #Очищаємо таблиці

conn.close()