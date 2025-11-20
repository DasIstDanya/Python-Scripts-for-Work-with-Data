# Импортируем необходимые библиотеки
import oracledb
import os
import sys

# <editor-fold desc="Для работы кода исправления не требуются, все пользовательские данные нужно вводить после запуска">

# Приём параметров для подключения к базе данных (БД) - нужно корректно задать свои параметры подключения
print("\nДля начала процедуры выгрузки заполните поля ниже:")
owner = input("\nВведите имя пользователя / схемы БД: ")
host = input("\nВведите хост БД: ")
port = input("\nВведите порт БД: ")
service_name = input("\nВведите имя сервиса БД: ")
user = input("\nВведите логин для входа в БД: ")
password = input("\nВведите пароль для входа БД: ")

# Объявляем переменную подключения к БД и задаем пустой курсор выборки данных
conn = None
cursor = None

# Форматируем параметры подключения к БД
owner = (f'{owner}')                         # Имя пользователя
dsn_tns = oracledb.makedsn(
    host=(f'{host}'),                        # Хост
    port=(f'{port}'),                        # Порт
    service_name=(f'{service_name}')         # Имя сервиса
)

# Открываем подключение к БД
conn = oracledb.connect(
    user=(f'{user}'),         # Логин
    password=(f'{password}'), # Пароль
    dsn=dsn_tns
)

print("\nПрогресс экспорта:")

# Определяем функции python, которые будут выгружать разные скрипты пользователя БД в отдельные папки

# Выгрузка типов
def export_oracle_types(output_dir):
    try:
        cursor = conn.cursor() # Открываем курсор

        # Проверяем наличие / создаем директорию для вывода
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Оформляем SQL-запрос для получения списка всех типов
        query = f"""
                SELECT DISTINCT OBJECT_NAME
                FROM ALL_OBJECTS 
                WHERE OBJECT_TYPE = 'TYPE'
                AND OWNER = '{owner}'
                """

        # Получаем список типов
        cursor.execute(query)
        type_names = cursor.fetchall()

        # Проверяем, что типы найдены
        if not type_names:
            print("Типы по заданным шаблонам не найдены!")
            return

        # Экспортируем каждый тип
        print("\nТипы:")
        for type in type_names:
            type_name = type[0]
            type_name_lower = type_name.lower()

            # SQL-запросы для спецификации и тела
            spec_query = f"""
                    SELECT TEXT 
                    FROM ALL_SOURCE 
                    WHERE LOWER(NAME) = '{type_name_lower}' 
                    AND TYPE = 'TYPE' 
                    ORDER BY LINE
                    """
            body_query = f"""
                    SELECT TEXT 
                    FROM ALL_SOURCE 
                    WHERE LOWER(NAME) = '{type_name_lower}' 
                    AND TYPE = 'TYPE BODY' 
                    ORDER BY LINE
                    """

            # Проверяем существование спецификации
            cursor.execute(spec_query)
            spec_lines = cursor.fetchall()
            if not spec_lines:
                print(f"Спецификация {type_name} не найдена!")
                continue

            # Экспорт спецификации
            spec_file_path = os.path.join(output_dir, f'{type_name_lower}.sql')
            with open(spec_file_path, 'w', encoding='utf-8') as f:
                f.write('CREATE OR REPLACE ')
                for line in spec_lines:
                    f.write(line[0].rstrip() + '\n')

            # Выводим название файла со спецификацией типа
            print(f"{type_name_lower}")

            # Проверяем существование тела типа
            cursor.execute(body_query)
            body_lines = cursor.fetchall()
            if not body_lines:
                print(f"Тело {type_name} не найдено!")
                continue

            # Экспорт тела типа
            body_file_path = os.path.join(output_dir, f'{type_name_lower}_body.sql')
            with open(body_file_path, 'w', encoding='utf-8') as f:
                f.write('CREATE OR REPLACE ')
                for line in body_lines:
                    f.write(line[0].rstrip() + '\n')

            # Выводим название файла с телом типа
            print(f"{type_name_lower}_body")

    # В случае ошибок выводим сообщения
    except oracledb.DatabaseError as e:
        print(f"Ошибка базы данных: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        sys.exit(1)

    # Закрываем курсор
    finally:
        if cursor:
            cursor.close()

# Выгрузка триггеров
def export_oracle_triggers(output_dir):
    try:
        cursor = conn.cursor() # Открываем курсор

        # Проверяем наличие / создаем директорию для вывода
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Оформляем SQL-запрос для получения списка всех триггеров
        query = f"""
                SELECT DISTINCT OBJECT_NAME 
                FROM ALL_OBJECTS 
                WHERE OBJECT_TYPE = 'TRIGGER' 
                AND OWNER = '{owner}'
                """

        # Получаем список триггеров
        cursor.execute(query)
        trigger_names = cursor.fetchall()

        # Проверяем, что триггеры найдены
        if not trigger_names:
            print("Триггеры по заданным шаблонам не найдены!")
            return

        # Экспортируем каждый триггер
        print("\nТриггеры:")
        for trigger in trigger_names:
            trigger_name = trigger[0]
            trigger_name_lower = trigger_name.lower()

            # SQL-запросы для текста триггеров
            query = f"""
                    SELECT TEXT 
                    FROM ALL_SOURCE 
                    WHERE LOWER(NAME) = '{trigger_name_lower}' 
                    AND TYPE = 'TRIGGER' 
                    ORDER BY LINE
                    """

            # Проверяем существование триггера
            cursor.execute(query)
            lines = cursor.fetchall()
            if not lines:
                print(f"Триггер {trigger_name} не найден!")
                continue

            # Экспорт триггера
            file_path = os.path.join(output_dir, f'{trigger_name_lower}.sql')
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('CREATE OR REPLACE ')
                for line in lines:
                    f.write(line[0].rstrip() + '\n')

            # Выводим название файла с триггером
            print(f"{trigger_name_lower}")

    # В случае ошибок выводим сообщения
    except oracledb.DatabaseError as e:
        print(f"Ошибка базы данных: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        sys.exit(1)

    # Закрываем курсор
    finally:
        if cursor:
            cursor.close()

# Выгрузка функций
def export_oracle_functions(output_dir):
    try:
        cursor = conn.cursor() # Открываем курсор

        # Проверяем наличие / создаем директорию для вывода
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # SQL-запрос для получения списка всех функций
        query = f"""
                SELECT DISTINCT OBJECT_NAME 
                FROM ALL_OBJECTS 
                WHERE OBJECT_TYPE = 'FUNCTION' 
                AND OWNER = '{owner}'
                """

        # Получаем список функций
        cursor.execute(query)
        function_names = cursor.fetchall()

        # Проверяем, что функции найдены
        if not function_names:
            print("Функции по заданным шаблонам не найдены!")
            return

        # Экспортируем каждую функцию
        print("\nФункции:")
        for function in function_names:
            function_name = function[0]
            function_name_lower = function_name.lower()

            # SQL-запросы для текста функций
            query = f"""
                    SELECT TEXT 
                    FROM ALL_SOURCE 
                    WHERE LOWER(NAME) = '{function_name_lower}' 
                    AND TYPE = 'FUNCTION' 
                    ORDER BY LINE
                    """

            # Проверяем существование функции
            cursor.execute(query)
            lines = cursor.fetchall()
            if not lines:
                print(f"Функция {function_name} не найдена!")
                continue

            # Экспорт функции
            file_path = os.path.join(output_dir, f'{function_name_lower}.sql')
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('CREATE OR REPLACE ')
                for line in lines:
                    f.write(line[0].rstrip() + '\n')

            # Выводим название файла с функцией
            print(f"{function_name_lower}")

    # В случае ошибок выводим сообщения
    except oracledb.DatabaseError as e:
        print(f"Ошибка базы данных: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        sys.exit(1)

    # Закрываем курсор
    finally:
        if cursor:
            cursor.close()

# Выгрузка процедур
def export_oracle_procedures(output_dir):
    try:
        cursor = conn.cursor() # Открываем курсор

        # Проверяем наличие / создаем директорию для вывода
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # SQL-запрос для получения списка всех процедур
        query = f"""
                SELECT DISTINCT OBJECT_NAME 
                FROM ALL_OBJECTS 
                WHERE OBJECT_TYPE = 'PROCEDURE' 
                AND OWNER = '{owner}'
                """

        # Получаем список процедур
        cursor.execute(query)
        procedure_names = cursor.fetchall()

        # Проверяем, что процедуры найдены
        if not procedure_names:
            print("Процедуры по заданным шаблонам не найдены!")
            return

        # Экспортируем каждую процедуру
        print("\nПроцедуры:")
        for procedure in procedure_names:
            procedure_name = procedure[0]
            procedure_name_lower = procedure_name.lower()

            # SQL-запросы для текста процедур
            query = f"""
                    SELECT TEXT 
                    FROM ALL_SOURCE 
                    WHERE LOWER(NAME) = '{procedure_name_lower}' 
                    AND TYPE = 'PROCEDURE' 
                    ORDER BY LINE
                    """

            # Проверяем существование процедуры
            cursor.execute(query)
            lines = cursor.fetchall()
            if not lines:
                print(f"Процедура {procedure_name} не найдена!")
                continue

            # Экспорт процедуры
            file_path = os.path.join(output_dir, f'{procedure_name_lower}.sql')
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('CREATE OR REPLACE ')
                for line in lines:
                    f.write(line[0].rstrip() + '\n')

            # Выводим название файла с процедурой
            print(f"{procedure_name_lower}")

    # В случае ошибок выводим сообщения
    except oracledb.DatabaseError as e:
        print(f"Ошибка базы данных: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        sys.exit(1)

    # Закрываем курсор
    finally:
        if cursor:
            cursor.close()

# Выгрузка пакетов
def export_oracle_packages(output_dir):
    try:
        cursor = conn.cursor() # Открываем курсор

        # Проверяем наличие / создаем директорию для вывода
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # SQL-запрос для получения списка всех пакетов
        query = f"""
                SELECT DISTINCT OBJECT_NAME
                FROM ALL_OBJECTS 
                WHERE OBJECT_TYPE = 'PACKAGE'
                AND OWNER = '{owner}'
                """

        # Получаем список пакетов
        cursor.execute(query)
        package_names = cursor.fetchall()

        # Проверяем, что пакеты найдены
        if not package_names:
            print("Пакеты по заданным шаблонам не найдены!")
            return

        # Экспортируем каждый пакет
        print("\nПакеты:")
        for package in package_names:
            package_name = package[0]
            package_name_lower = package_name.lower()

            # SQL-запросы для спецификации и тела
            spec_query = f"""
                    SELECT TEXT 
                    FROM ALL_SOURCE 
                    WHERE LOWER(NAME) = '{package_name_lower}' 
                    AND TYPE = 'PACKAGE' 
                    ORDER BY LINE
                    """
            body_query = f"""
                    SELECT TEXT 
                    FROM ALL_SOURCE 
                    WHERE LOWER(NAME) = '{package_name_lower}' 
                    AND TYPE = 'PACKAGE BODY' 
                    ORDER BY LINE
                    """

            # Проверяем существование спецификации пакета
            cursor.execute(spec_query)
            spec_lines = cursor.fetchall()

            if not spec_lines:
                print(f"Спецификация {package_name} не найдена!")
                continue

            # Экспорт спецификации
            spec_file_path = os.path.join(output_dir, f'{package_name_lower}.sql')
            with open(spec_file_path, 'w', encoding='utf-8') as f:
                f.write('CREATE OR REPLACE ')
                for line in spec_lines:
                    f.write(line[0].rstrip() + '\n')

            # Выводим название файла со спецификацией пакета
            print(f"{package_name_lower}")

            # Проверяем существование тела пакета
            cursor.execute(body_query)
            body_lines = cursor.fetchall()

            if not body_lines:
                print(f"Тело {package_name} не найдено!")
                continue

            # Экспорт тела пакета
            body_file_path = os.path.join(output_dir, f'{package_name_lower}_body.sql')
            with open(body_file_path, 'w', encoding='utf-8') as f:
                f.write('CREATE OR REPLACE ')
                for line in body_lines:
                    f.write(line[0].rstrip() + '\n')

            # Выводим название файла с телом пакета
            print(f"{package_name_lower}_body")

    # В случае ошибок выводим сообщения
    except oracledb.DatabaseError as e:
        print(f"Ошибка базы данных: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        sys.exit(1)

    # Закрываем курсор
    finally:
        if cursor:
            cursor.close()

# Здесь непосредственно запускаем процесс выгрузки
if __name__ == "__main__":
    OUTPUT_DIRECTORY = 'types'  # Изначально определяем директорию под сохранения типов
    export_oracle_types(OUTPUT_DIRECTORY) # Запускаем функцию выгрузки типов
    OUTPUT_DIRECTORY = 'triggers'  # Переопределяем директорию для сохранения триггеров
    export_oracle_triggers(OUTPUT_DIRECTORY)
    # Далее по аналогии с функциями, процедурами и пакетами
    OUTPUT_DIRECTORY = 'functions'
    export_oracle_functions(OUTPUT_DIRECTORY)
    OUTPUT_DIRECTORY = 'procedures'
    export_oracle_procedures(OUTPUT_DIRECTORY)
    OUTPUT_DIRECTORY = 'packages'
    export_oracle_packages(OUTPUT_DIRECTORY)

# Закрываем подключение к БД
if cursor:
    cursor.close()

# </editor-fold>
