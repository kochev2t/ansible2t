#!/bin/python3.9
# coding: utf-8

# Стандартные библиотеки и фреймворки
import hashlib
import datetime
import argparse
import glob
import platform, os
# Внешние библиотеки и фреймворки
import gitlab


# Ветка репозитория
BRANCH = ['main']
# Сообщение коммита
COMMIT_MESSAGE = [f'Загружена новая версия проекта на {datetime.datetime.strftime(datetime.datetime.now(), "%y-%m-%d %H:%M:%S")}']

# Маски путей к файлам конфигурации кластера 1C
FILES_MASKS_PATHS = [
            "/opt/1cv8/scripts/scripts/*", 
            "/var/www/*/default.vrd", 
            "/var/1C/*540/1cv8wsrv.lst",
            "/var/1C/*540/reg_*541/*.lst",
            "/etc/systemd/system/srv1cv8-*.service",
            "/etc/systemd/system/ras-*.service",
            "/opt/1cv8/conf/*",
           ]

# Получить hostname сервера как список
def get_hostname():
    if platform.system() == "Windows":
        return [platform.uname().node]
    else:
        return [os.uname()[1]]

# Получить объект gitlab для предоставления доступа к хосту GitLab для использования
def get_gitlab_host(gitlab_host, private_token, ssl_verify):
    return gitlab.Gitlab(gitlab_host, private_token=private_token, ssl_verify=ssl_verify)

# Получить проект по ID для использования API сервера GitLab
def get_project_by_id(gitlab_space, project_id):
    return gitlab_space.projects.get(project_id)

# Получить проект по имени для использования API сервера GitLab
def get_project_by_name(gitlab_space, project_name):
    is_exist_project = False
    project_id = None
    # Поиск только по имени
    print("Имя проекта для поиска: '", project_name, "'", sep='')
    # Ищем все доступные нам приватные группы по токену
    groups = gitlab_space.groups.list(iterator=True, visibility='private', all_available=True)
    # Выводим все доступные нам группы
    print("Все доступные по токену группы: ")
    groups = list(groups) # Преобразование в list на случай, если мы получим RestObjectList
    for i in range(len(groups)):
        print(f"\tИмя группы: {groups[i].name}, ID группы: {groups[i].id}")
    if len(groups) == 0: # Проверка на наличие доступа к группам
        print("Проект не входит в группу или у вас недостаточно прав")
        exit(1)
    # Ищем первую группу в которую входит проект с именем хоста
    print("Найденные проекты групп:")
    for group in groups:
        # Выводим группу
        print("\tГруппа:", group.name, group.id)
        # Ищем проекты по имени в доступной группе 
        projects = group.projects.list(iterator=True, search=project_name)
        projects = list(projects) # Преобразование в list на случай, если мы получим RestObjectList
        if len(projects) == 0: # Продолжаем пока не найдем полную группу
            continue
        # Выводим все доступные нам проекты на группу
        for i in range(len(projects)):
            print("\t\tПроект:", projects[i].name, projects[i].id)
            if projects[i].name == project_name:
                project_id = projects[i].id
                is_exist_project = True
                break
        if is_exist_project: # Выходим из цикла если нашли проект
            break
    print()
    if project_id is None:
        print("Такого проекта нет или у вас недостаточно прав доступа")
        exit(1)
    # Получаем доступ к объекту проекта, которым можно манипулировать
    project = get_project_by_id(gitlab_space, project_id)
    return project

# Функция создания хэша из локального файла
def generate_file_hash_sha256(file_path):

    hasher = hashlib.sha256()
    with open(file_path, 'rb') as file:
        buf = file.read()
        # Нормализация окончаний строк
        # из CRLF к LF как сохраняет GitLab
        normolized_buf = buf.replace(b'\r\n', b'\n')
        hasher.update(normolized_buf)
    return hasher.hexdigest()

# Получить хэш файла из удаленного репозитория
def get_remote_file_hash_sha256(project, file_path, branch):
    # Пытаемся получить хэш sha256 указанного файла
    # Если такого файла нет, то мы возвращаем None
    try:
        remote_file = project.files.get(file_path=file_path, ref=branch)
        remote_file_hash = remote_file.content_sha256
    except gitlab.exceptions.GitlabGetError as err:
        remote_file_hash = None
    return remote_file_hash

# Получить из списка масок конфигов список
# путей к конфигам
def config_list(files_masks_paths):
    result = []
    for pattern in files_masks_paths:
        # Используем glob.glob для поиска файлов и директорий по шаблону
        for path in glob.glob(pattern, recursive=True):
            if path.find(".keytab") == -1:
                result.append(path)
    return result

# Загрузить файл(ы), указанные в списке по маскам
def upload_file(project, files_path_list, branch, commit_message):
    # Измененные конфигурации
    modified_configs = []
    # Список действий при создании коммита для каждого файла
    actions = []
    print("Проверка файлов на наличие и изменения для включения в коммит:", end="\n\n")
    for config in files_path_list:
        # Получить хэш локального файла
        local_file_hash = generate_file_hash_sha256(config)
        remote_path = config
        # В случае абсолютного путя убираем начальный слэш для удаленного
        if config[0] == "/":
            remote_path = config[1:]
        # Получить хэш файла из удаленного репозитория GitLab
        remote_file_hash = get_remote_file_hash_sha256(project, remote_path, branch)

        # Сравнение хэшей локального и удаленного файла
        print(config, remote_file_hash, local_file_hash, sep='\n')
        
        # Выбор возможного действия 'создать' или 'обновить'
        if remote_file_hash is None:
            action = 'create'
            message = "Локальный файл не был найден в репозитории. Добавляю в коммит."
        else:
            action = 'update'
            message = "Локальный файл различается с удаленным. Добавляю в коммит."
        # Проверка на наличие изменений
        if remote_file_hash != local_file_hash:
            actions.append({
                'action': action,
                'file_path': remote_path,
                'content': open(config, 'r').read()
            })
            modified_configs.append(config)
            # Сообщаем что изменений нет и обновление не требуется
            print(message, end="\n\n")
        else:
            # Сообщаем что изменений нет и обновление не требуется
            print(f"Не было обнаружено никаких изменений в файле '{config}'. Пропускаю добавление в коммит.", end="\n\n")
    if not len(actions) == 0:
        # Собираем все данные для создания коммита
        data = {
            'branch': branch,
            'commit_message': commit_message,
            # В листе actions указываем все действия над каждым файлом
            'actions': actions
        }
        print(f"Измененные конфиги (количество - {len(actions)}):", end="\n\n")
        for modified_config in modified_configs:
            print(modified_config)
        print()
        try:
            # Создаем коммит
            project.commits.create(data)
        except gitlab.exceptions.GitlabCreateError as err:
            print(err)
            exit(1)
        print("Конфиги залиты в GitLab.")

# Скачать файл из последней версии коммита
def download_file(project, remote_file_path, file_path, branch):
    local_file = file_path
    # Дата выгрузки
    TARGET_DATE = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    # Получение списка коммитов для файла
    commits = project.commits.list(ref_name=branch, all=True)
    if commits != []:
        # Выбираем последний коммит
        target_commit = commits[0]
        
        # Получение файла из найденного коммита
        # !!Разное открытие для бинарного и строкового формата
        try:
            file = project.files.get(file_path=remote_file_path, ref=target_commit.id)
            file_content = file.decode().decode()
        except gitlab.exceptions.GitlabGetError as e:
            print(f"Ошибка при получении файла: {e}")
            exit(1)
        # Сохранение файла локально
        with open(file_path, 'w') as local_file:
            local_file.write(file_content)
        
        print(f"Файл '{remote_file_path}' успешно скачан на дату {TARGET_DATE} и сохранен как '{file_path}'")
    else:
        print(f"Ошибка при получении файла: Не найдено ни одного коммита")
        exit(1)

# Точка запуска
def main(args):
    # 1) Получаем доступ к хосту GitLab
    print("Получаю доступ к хосту GitLab..")
    
    gitlab_space = get_gitlab_host(args.host_url[0], args.private_token[0], False)

    # 2) Получение доступа к проекту GitLab
    print("Получаю доступ к проекту..", end="\n\n")
    try:
        # Ищем проект по ID проекта, игнорируя поиск по имени
        if args.project_id is not None:
            if args.project_id != []:
                project = get_project_by_id(gitlab_space, args.project_id[0])
            else:
                print("Укажите значение ID проекта для флага -id/--project-id")
                exit(1)
        # По умолчанию ищем проект по имени в GitLab пространстве
        else:
            project = get_project_by_name(gitlab_space, args.project_name[0])
    # Обрабатываем ошибку авторизации
    except gitlab.exceptions.GitlabAuthenticationError as err:
        print(err)
        exit(1)
    
    # 4) Устанавливаем значения, если для флага -up/--upload
    # указаны другие маски путей к конфигурациям
    # и запускаем загрузку файлов в репозиторий GitLab
    if args.upload is not None:
        if args.upload != []:
            files_masks_paths = args.upload
        else:
            files_masks_paths = FILES_MASKS_PATHS
        # Получение списка путей к конфигам
        paths_to_configs = config_list(files_masks_paths)
        # Вывод полученных путей
        print(f"Полученные пути к конфигурациям кластера 1C (количество - {len(paths_to_configs)}):", end="\n\n")
        for path in paths_to_configs:
            print(path)
        print()
        # Выгрузка файлов по заданному списку путей к конфигам
        upload_file(project, paths_to_configs, args.branch[0], args.commit_message[0])
    
    # 5) Устанавливаем значения, если был флаг -dw/--download
    # Запускаем скачивание файла из репозитория GitLab в указанную директорию
    # с указанным именем
    if args.download is not None:
        remote_file_path = args.download[0]
        file_path = args.download[1]
        download_file(project, remote_file_path, file_path, args.branch[0])


if __name__ == "__main__":
    
    ###### Верхнеуровневая абстракция ####
    ###### для парсинга аргументов    ####
    parser = argparse.ArgumentParser(prog="Скрипт для версионирования конфигурационных файлов кластера 1C на запущенном сервере")

    ##### Обязательные флаги        ######

    # -hurl / --host-url - URL серврвера в GitLab
    parser.add_argument('-hurl', '--host-url', required=True, nargs=1, help='Указать URL хоста для репозитория GitLab.')
    # -pt / --private-token - Приватный токен доступа к репозиторию (Должен иметь права доступа в группу, где расположен проект)
    parser.add_argument('-pt', '--private-token', required=True, nargs=1, help='Указать приватный токен для доступа к репозиторию.')

    ##### Необязательные флаги      ######

    # -n / --project-name - имя проекта
    parser.add_argument('-n', '--project-name', nargs=1, default=get_hostname(), 
                        help="""Указать имя проекта на который/с которого требуется загружать/выгружать.
                        По умолчанию в качестве имени ставится имя hostname""")
    # -id / --project-id - ID проекта
    parser.add_argument('-id', '--project-id', nargs=1, help='Указать ID проекта на который/с которого требуется загружать/выгружать.')
    # -b / --branch - Выбор ветки в репозитории
    parser.add_argument('-b', '--branch', nargs=1, default=BRANCH, 
                        help="""Указать название ветки загрузки/выгрузки с/на репозиторий GitLab.\n
                                Например, sudo ./versioning_configs --branch 'master'\n
                                По умолчанию значение 'main'.""")
    # -cm / --commit-message - Установить сообщение коммита
    parser.add_argument('-cm', '--commit-message', nargs=1, default=COMMIT_MESSAGE, 
                        help="""Установить сообщение коммита.\n
                        По умолчанию сообщение коммита 'Загружена новая версия проекта на <Текущая дата и время>'""")
    # -up / --upload - Загрузка фалов конфигурации (можно указать другие пути, но по умолчанию указаны стандартные)
    parser.add_argument('-up', '--upload', nargs='*', 
                        help='Загрузить файлы конфигурации с сервера на репозиторий GitLab по указанным маскам путей к ним.')
    # -dw / --download - Скачать файл конфигруации по указанному удаленному пути на указанный локальный путь
    # (/remote/path/file.extension, /local/path/file.extension)
    parser.add_argument('-dw', '--download', nargs=2, default=None, 
                        help='Скачать файл конфигурации по указанному удаленному пути на указанный путь.')
    
    # Собираем все полученные флаги и значения для них
    # !!ЕСЛИ НЕОБХОДИМЫЕ ФЛАГИ ИЛИ ИХ ЗНАЧЕНИЯ НЕ БЫЛИ УКАЗАНЫ,
    # !!ТО НА ЭТОМ ШАГЕ СКРИПТ ЗАВЕРШАЕТСЯ С ОПОВЕЩЕНИЕМ
    # !!О НЕККОРЕКТНОМ ИСПОЛЬЗОВАНИИ 
    args = parser.parse_args()

    main(args)
