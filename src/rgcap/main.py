from os import makedirs, path

from lib.functions import load_config
from lib.mainloop import mainloop
from lib.rg_controller import RGController


def main():
    config = load_config()

    db_route = path.expanduser(config["db_route"])

    db_route_dir = path.dirname(db_route)

    makedirs(db_route_dir, exist_ok=True)

    connection = RGController(
        config,
        autocommit=True,
    )

    result = connection.create_db()

    if result == 0:
        print("Base de datos creada con éxito!\n")

    try:
        mainloop(connection)

    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
