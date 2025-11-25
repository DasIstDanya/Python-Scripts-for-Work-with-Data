# Импортируем необходимые библиотеки
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# <editor-fold desc="Для работы кода исправления не требуются, все пользовательские данные нужно вводить после запуска">

# Вводим параметры подключения БД через SQLAlchemy. Owner нужен для execute-запросов ниже
print("\nДля начала процедуры выгрузки заполните поля ниже:")
owner = input("\nВведите имя пользователя / схемы БД: ")
host = input("\nВведите хост БД: ")
port = input("\nВведите порт БД: ")
service_name = input("\nВведите имя сервиса БД: ")
user = input("\nВведите логин для входа в БД: ")
password = input("\nВведите пароль для входа БД: ")

connection_string = f'oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service_name}'

engine = create_engine(connection_string)

objects1 = ("types", "packages") # Есть спецификация и тело

objects2 = ("functions", "triggers", "procedures") # Есть только текст

try:
    with engine.connect() as connection:
        print("\nПрогресс экспорта:")
        # Выгружаем объекты со спецификациями и телами
        for object1 in objects1:
            # Определяем директорию
            output_dir = object1
            object = output_dir[:-1].upper()

            # Получаем список объект
            list = connection.execute(text(f"""
                            SELECT DISTINCT OBJECT_NAME
                            FROM ALL_OBJECTS 
                            WHERE OBJECT_TYPE = '{object}'
                            AND OWNER = '{owner}'"""))
            names = list.fetchall()

            # Проверяем, что объекты найдены
            if not names:
                print(f"{object1} по заданным шаблонам не найдены!")
            else:
                # Проверяем наличие / создаем директорию для вывода
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                # Экспортируем каждый объект
                print(f"\n{object1}:")
                for name in names:
                    name = name[0]
                    name_lower = name.lower()

                    spec_query = connection.execute(text(f"""
                                            SELECT TEXT 
                                            FROM ALL_SOURCE 
                                            WHERE LOWER(NAME) = '{name_lower}' 
                                            AND TYPE = '{object}' 
                                            ORDER BY LINE
                                            """))
                    spec_lines = spec_query.fetchall()
                    if not spec_lines:
                        print(f"Спецификация {name} не найдена!")
                        continue
                    spec_file_path = os.path.join(output_dir, f'{name_lower}.sql')
                    with open(spec_file_path, 'w', encoding='utf-8') as f:
                        f.write('CREATE OR REPLACE ')
                        for line in spec_lines:
                            f.write(line[0].rstrip() + '\n')
                    print(f"{name_lower}")

                    body_query = connection.execute(text(f"""
                                            SELECT TEXT 
                                            FROM ALL_SOURCE 
                                            WHERE LOWER(NAME) = '{name_lower}' 
                                            AND TYPE = '{object} BODY' 
                                            ORDER BY LINE
                                                        """))
                    body_lines = body_query.fetchall()
                    if not body_lines:
                        print(f"Тело {name} не найдено!")
                        continue
                    body_file_path = os.path.join(output_dir, f'{name_lower}_body.sql')
                    with open(body_file_path, 'w', encoding='utf-8') as f:
                        f.write('CREATE OR REPLACE ')
                        for line in body_lines:
                            f.write(line[0].rstrip() + '\n')
                    print(f"{name_lower}_body")
        # Выгружаем "цельные" объекты
        for object2 in objects2:
            # Определяем директорию
            output_dir = object2
            object = output_dir[:-1].upper()
            # Получаем список объектов
            list = connection.execute(text(f"""
                            SELECT DISTINCT OBJECT_NAME
                            FROM ALL_OBJECTS 
                            WHERE OBJECT_TYPE = '{object}'
                            AND OWNER = '{owner}'"""))
            names = list.fetchall()
            # Проверяем, что объекты найдены
            if not names:
                print(f"{object2} по заданным шаблонам не найдены!")
            else:
                # Проверяем наличие / создаем директорию для вывода
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                # Экспортируем каждый объект
                print(f"\n{object2}:")
                for name in names:
                    name = name[0]
                    name_lower = name.lower()
                    query = connection.execute(text(f"""
                                            SELECT TEXT 
                                            FROM ALL_SOURCE 
                                            WHERE LOWER(NAME) = '{name_lower}' 
                                            AND TYPE = '{object}' 
                                            ORDER BY LINE
                                            """))
                    lines = query.fetchall()
                    if not lines:
                        print(f"Объект {name} не найден!")
                        continue
                    file_path = os.path.join(output_dir, f'{name_lower}.sql')
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write('CREATE OR REPLACE ')
                        for line in lines:
                            f.write(line[0].rstrip() + '\n')
                    print(f"{name_lower}")

except SQLAlchemyError as e:
    print(f"Ошибка при работе с БД: {e}")
except Exception as e:
    print(f"Непредвиденная ошибка: {e}")
finally:
    engine.dispose()
# </editor-fold>
