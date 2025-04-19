import csv
import psycopg2


def connect_db():
    return psycopg2.connect(
        dbname="postgres", user="postgres", password="Biba2006", host="localhost"
    )


def create_table():
    try:
        connection = connect_db()
        cursor = connection.cursor()
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS phonebook (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                phone VARCHAR(20) UNIQUE
            );
        """)

        cursor.execute("""
            CREATE OR REPLACE PROCEDURE insert_or_update_user(
                in_first_name VARCHAR,
                in_last_name VARCHAR,
                in_phone VARCHAR
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF EXISTS (SELECT 1 FROM phonebook WHERE phone = in_phone) THEN
                    UPDATE phonebook SET first_name = in_first_name, last_name = in_last_name WHERE phone = in_phone;
                ELSE
                    INSERT INTO phonebook(first_name, last_name, phone)
                    VALUES (in_first_name, in_last_name, in_phone);
                END IF;
            END;
            $$;
        """)

        cursor.execute("""
            CREATE OR REPLACE PROCEDURE insert_many_users(
                names TEXT[], surnames TEXT[], phones TEXT[]
            )
            LANGUAGE plpgsql
            AS $$
            DECLARE
                i INT := 1;
                invalid_phones TEXT[] := ARRAY[]::TEXT[];
            BEGIN
                WHILE i <= array_length(names, 1) LOOP
                    IF phones[i] ~ '^\\+?[0-9]{10,15}$' THEN
                        CALL insert_or_update_user(names[i], surnames[i], phones[i]);
                    ELSE
                        invalid_phones := array_append(invalid_phones, phones[i]);
                    END IF;
                    i := i + 1;
                END LOOP;
                -- Return invalid phone numbers using RAISE NOTICE
                RAISE NOTICE 'Invalid phones: %', invalid_phones;
            END;
            $$;
        """)

        cursor.execute("""
            CREATE OR REPLACE FUNCTION search_by_pattern(pattern TEXT)
            RETURNS TABLE(id INT, first_name VARCHAR, last_name VARCHAR, phone VARCHAR)
            LANGUAGE sql
            AS $$ 
                SELECT * FROM phonebook
                WHERE first_name ILIKE '%' || pattern || '%'
                   OR last_name ILIKE '%' || pattern || '%'
                   OR phone ILIKE '%' || pattern || '%';
            $$;
        """)

        cursor.execute("""
            CREATE OR REPLACE FUNCTION get_paginated_users(limit_count INT, offset_count INT)
            RETURNS TABLE(id INT, first_name VARCHAR, last_name VARCHAR, phone VARCHAR)
            LANGUAGE sql
            AS $$ 
                SELECT * FROM phonebook ORDER BY id LIMIT limit_count OFFSET offset_count;
            $$;
        """)

        cursor.execute("""
            CREATE OR REPLACE PROCEDURE delete_by_name_or_phone(query TEXT)
            LANGUAGE plpgsql
            AS $$ 
            BEGIN
                DELETE FROM phonebook WHERE phone = query OR first_name = query;
            END;
            $$;
        """)

        connection.commit()
        print("Table and procedures/functions created successfully.")
    except Exception as error:
        print(f"Error creating table: {error}")
    finally:
        cursor.close()
        connection.close()


def insert_data_from_csv(file_name):
    try:
        connection = connect_db()
        cursor = connection.cursor()
        with open(file_name, 'r', encoding='utf-8') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)
            for row in csvreader:
                if len(row) < 3:
                    continue
                first_name, last_name, phone = row
                cursor.execute(""" 
                    CALL insert_or_update_user(%s, %s, %s)
                """, (first_name, last_name, phone))
        connection.commit()
        print("Data inserted successfully from CSV!")
    except Exception as error:
        print(f"Error inserting data from CSV: {error}")
    finally:
        cursor.close()
        connection.close()


def insert_data_from_console():
    first_name = input("Enter first name: ").strip()
    last_name = input("Enter last name: ").strip()
    phone = input("Enter phone number: ").strip()

    if not first_name or not phone:
        print("First name and phone are required!")
        return

    try:
        connection = connect_db()
        cursor = connection.cursor()
        cursor.execute("CALL insert_or_update_user(%s, %s, %s)", (first_name, last_name, phone))
        connection.commit()
        print("Data inserted/updated successfully!")
    except Exception as error:
        print(f"Error inserting data from console: {error}")
    finally:
        cursor.close()
        connection.close()


def insert_many_users_from_console():
    try:
        names = input("Enter first names separated by commas: ").split(',')
        surnames = input("Enter last names separated by commas: ").split(',')
        phones = input("Enter phone numbers separated by commas: ").split(',')

        if not (len(names) == len(surnames) == len(phones)):
            print("Error: All arrays must have the same length.")
            return

        connection = connect_db()
        cursor = connection.cursor()
        cursor.execute("CALL insert_many_users(%s, %s, %s)", (names, surnames, phones))
        connection.commit()

        print("Users inserted from console.")

        # Show only newly inserted users based on phones
        cursor.execute("SELECT id, first_name, last_name, phone FROM phonebook WHERE phone = ANY(%s);", (phones,))
        rows = cursor.fetchall()

        if rows:
            print("Inserted users:")
            for row in rows:
                print(row)
        else:
            print("No new users inserted.")
    except Exception as error:
        print(f"Error inserting many users from console: {error}")
    finally:
        cursor.close()
        connection.close()


def update_data_console():
    phone = input("Enter the phone number to update: ").strip()
    new_first_name = input("Enter new first name (leave blank to keep current): ").strip()
    new_last_name = input("Enter new last name (leave blank to keep current): ").strip()

    try:
        connection = connect_db()
        cursor = connection.cursor()

        # Если введены новые данные, обновляем их
        if new_first_name and new_last_name:
            cursor.execute("CALL insert_or_update_user(%s, %s, %s)", (new_first_name, new_last_name, phone))
        elif new_first_name:  # Обновляем только имя
            cursor.execute("CALL insert_or_update_user(%s, (SELECT last_name FROM phonebook WHERE phone = %s), %s)",
                           (new_first_name, phone, phone))
        elif new_last_name:  # Обновляем только фамилию
            cursor.execute("CALL insert_or_update_user((SELECT first_name FROM phonebook WHERE phone = %s), %s, %s)",
                           (phone, new_last_name, phone))
        else:
            print("No changes to update.")
            return

        connection.commit()
        print("Data updated successfully if phone number exists.")
    except Exception as error:
        print(f"Error updating data: {error}")
    finally:
        cursor.close()
        connection.close()


def query_data(filter_type=None, value=None):
    try:
        connection = connect_db()
        cursor = connection.cursor()
        if filter_type == "pattern":
            cursor.execute("SELECT * FROM search_by_pattern(%s)", (value,))
        elif filter_type == "pagination":
            limit = int(input("Enter limit: "))
            offset = int(input("Enter offset: "))
            cursor.execute("SELECT * FROM get_paginated_users(%s, %s)", (limit, offset))
        else:
            cursor.execute("SELECT * FROM phonebook")
        rows = cursor.fetchall()
        if not rows:
            print("No data found.")
        for row in rows:
            print(row)
    except Exception as error:
        print(f"Error querying data: {error}")
    finally:
        cursor.close()
        connection.close()


def delete_data(identifier):
    try:
        connection = connect_db()
        cursor = connection.cursor()
        cursor.execute("CALL delete_by_name_or_phone(%s)", (identifier,))
        connection.commit()
        print("Deleted successfully if data existed.")
    except Exception as error:
        print(f"Error deleting data: {error}")
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    while True:
        print("\nPhoneBook Menu:")
        print("1. Create table")
        print("2. Insert data from CSV")
        print("3. Insert data from console")
        print("4. Update data")
        print("5. Query data")
        print("6. Delete data")
        print("7. Insert many users from console")
        print("8. Delete users by pattern")
        print("9. Exit")

        choice = input("Enter your choice: ").strip()

        if choice == "1":
            create_table()
        elif choice == "2":
            file_name = input("Enter CSV file name: ")
            insert_data_from_csv(file_name)
        elif choice == "3":
            insert_data_from_console()
        elif choice == "4":
            update_data_console()  # Обновленная функция для обработки обновлений
        elif choice == "5":
            query_mode = input("Enter 'pattern' to search or 'pagination' for pages (or press Enter for all): ").strip()
            if query_mode == "pattern":
                value = input("Enter pattern: ").strip()
                query_data("pattern", value)
            elif query_mode == "pagination":
                query_data("pagination")
            else:
                query_data()
        elif choice == "6":
            value = input("Enter name or phone to delete: ").strip()
            delete_data(value)
        elif choice == "7":
            insert_many_users_from_console()
        elif choice == "8":
            pattern = input("Enter pattern to delete users by: ")
            delete_data(pattern)
        elif choice == "9":
            print("Exiting PhoneBook. Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")