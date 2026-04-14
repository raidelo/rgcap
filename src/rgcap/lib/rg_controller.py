from os import listdir, makedirs, remove, rmdir, path
import sqlite3

from matplotlib import pyplot
from pandas import read_sql, DataFrame
from seaborn import lineplot

from .functions import get_time_now


class RGController(sqlite3.Connection):

    __MAIN_TABLE = {
        "name": "blockchain",
        "columns": {
            "field": "fuente",
            "ammount": "cantidad",
            "datetime": "hora",
            "description": {
                "column_name": "description",
                "default_value": "",
            },
        },
    }
    __TOTAL_TABLE = {
        "name": "total",
        "prefix": "Total_",
        "columns": {
            "field": "fuente",
            "ammount": "cantidad",
            "datetime": "hora",
        },
    }

    main_name = "blockchain"
    total_name = "total"
    field_col_name = "fuente"
    ammount_col_name = "cantidad"
    datetime_col_name = "hora"
    description_col_name = "description"
    description_col_default_value = ""
    total_field_prefix = "Total_"

    def __init__(
        self,
        config: dict,
        *args,
        **kwargs,
    ):

        db_route = path.expanduser(config["db_route"])
        self.backups_folder = path.expanduser(config["backups"])

        self.default_field = config["default_field"]

        self.base_folder = path.dirname(db_route) + "/"
        self.db_name = path.basename(db_route).split(".")[0]

        self.fields = config["fields"]
        self.fields_ = self.__invert_fields(self.fields)

        super().__init__(db_route, *args, **kwargs)

    def create_db(self) -> int:
        """
        Crea la base de datos con las tablas `blockchain` y `total`. No hace nada si ya existe
        """
        try:
            # Crear ambas tablas
            self.__create_main()
            self.__create_total()
            return 0
        except sqlite3.OperationalError:
            return 1

    def __create_main(self):
        cursor = self.cursor()

        cursor.execute(
            "CREATE table %(table)s ( %(field)s TEXT NOT NULL, %(ammount)s REAL NOT NULL, %(datetime)s TEXT NOT NULL, id INTEGER PRIMARY KEY NOT NULL UNIQUE, %(description)s TEXT NOT NULL DEFAULT '%(description_default_value)s' )"
            % {
                "table": self.__MAIN_TABLE["name"],
                "field": self.__MAIN_TABLE["columns"]["field"],
                "ammount": self.__MAIN_TABLE["columns"]["ammount"],
                "datetime": self.__MAIN_TABLE["columns"]["datetime"],
                "description": self.__MAIN_TABLE["columns"]["description"][
                    "column_name"
                ],
                "description_default_value": self.__MAIN_TABLE["columns"][
                    "description"
                ]["default_value"],
            }
        )

        cursor.close()

    def __create_total(self):
        cursor = self.cursor()

        cursor.execute(
            "CREATE table %(table)s ( %(field)s TEXT NOT NULL, %(ammount)s REAL NOT NULL, %(datetime)s TEXT NOT NULL )"
            % {
                "table": self.__TOTAL_TABLE["name"],
                "field": self.__TOTAL_TABLE["columns"]["field"],
                "ammount": self.__TOTAL_TABLE["columns"]["ammount"],
                "datetime": self.__TOTAL_TABLE["columns"]["datetime"],
            }
        )

        datetime_now = get_time_now()

        # Crear contenido inicial de la tabla `total`

        cursor.execute(
            "INSERT INTO %(table)s (%(field)s, %(ammount)s, %(datetime)s) VALUES %(values)s"
            % {
                "table": self.__TOTAL_TABLE["name"],
                "field": self.__TOTAL_TABLE["columns"]["field"],
                "ammount": self.__TOTAL_TABLE["columns"]["ammount"],
                "datetime": self.__TOTAL_TABLE["columns"]["datetime"],
                "values": ", ".join(
                    [
                        "('%(field)s', 0, '%(datetime)s')"
                        % {
                            "field": self.__TOTAL_TABLE["prefix"] + field,
                            "datetime": datetime_now,
                        }
                        for field in self.fields
                    ]
                ),
            }
        )

        cursor.close()

    def __insert_into_main(self, field: str, ammount: float, description: str):

        cursor = self.cursor()

        cursor.execute(
            "INSERT INTO %(table)s (%(field_col_name)s, %(ammount_col_name)s, %(datetime_col_name)s, %(description_col_name)s) \
            VALUES ('%(field)s', %(ammount)s, '%(datetime)s', '%(description)s')"
            % {
                "table": self.__MAIN_TABLE["name"],
                "field_col_name": self.__MAIN_TABLE["columns"]["field"],
                "ammount_col_name": self.__MAIN_TABLE["columns"]["ammount"],
                "datetime_col_name": self.__MAIN_TABLE["columns"]["datetime"],
                "description_col_name": self.__MAIN_TABLE["columns"]["description"][
                    "column_name"
                ],
                "field": field,
                "ammount": ammount,
                "datetime": get_time_now(),
                "description": description,
            }
        )

        cursor.close()

    def __update_total(self, field: str, ammount: float):

        cursor = self.cursor()

        field = self.__TOTAL_TABLE["prefix"] + field

        cursor.execute(
            "SELECT %(ammount_col_name)s FROM %(table)s WHERE %(field_col_name)s='%(field)s'"
            % {
                "table": self.__TOTAL_TABLE["name"],
                "ammount_col_name": self.__TOTAL_TABLE["columns"]["ammount"],
                "field_col_name": self.__TOTAL_TABLE["columns"]["field"],
                "field": field,
            }
        )

        result = cursor.fetchall()

        total_table = self.__TOTAL_TABLE["name"]
        datetime_now = get_time_now()

        if result:
            new_ammount = result[0][0] + ammount

            cursor.execute(
                "UPDATE %(table)s SET %(ammount_col_name)s=%(ammount)s, %(datetime_col_name)s='%(datetime)s' WHERE %(field_col_name)s='%(field)s'"
                % {
                    "table": total_table,
                    "ammount": new_ammount,
                    "datetime": datetime_now,
                    "ammount_col_name": self.__TOTAL_TABLE["columns"]["ammount"],
                    "datetime_col_name": self.__TOTAL_TABLE["columns"]["datetime"],
                    "field_col_name": self.__TOTAL_TABLE["columns"]["field"],
                    "field": field,
                }
            )

        else:
            cursor.execute(
                "INSERT INTO %(table)s (%(field_col_name)s, %(ammount_col_name)s, %(datetime_col_name)s) VALUES ('%(field)s', %(ammount)s, '%(datetime)s')"
                % {
                    "table": self.__TOTAL_TABLE["name"],
                    "field_col_name": self.__TOTAL_TABLE["columns"]["field"],
                    "ammount_col_name": self.__TOTAL_TABLE["columns"]["ammount"],
                    "datetime_col_name": self.__TOTAL_TABLE["columns"]["datetime"],
                    "field": field,
                    "ammount": ammount,
                    "datetime": get_time_now(),
                }
            )

            self.__sort_total()

        cursor.close()

    def __sort_total(self):
        """
        Ordena la tabla `total` de la base de datos según el orden de los `fields` del archivo de configuración
        """

    def update_db(
        self,
        field: str,
        ammount: float,
        description: str = "",
        update_total: bool = True,
    ) -> int:
        """
        Actualiza la base de datos con los datos proporcionados.

        field:           el campo. debe ser una clave de la configuración
        ammount:         la cantidad ingresada o sustraída del campo
        description:     descripción de la operación
        update_total:    actualizar la tabla total automáticamente
        """

        assert isinstance(field, str)
        assert isinstance(ammount, (int, float)) and ammount not in [0, 0.0]
        assert isinstance(description, str)
        #
        # if ammount in [0, 0.0]:
        #     return 1

        field = field.upper()

        try:
            self.__insert_into_main(field, ammount, description)

            if update_total:
                self.__update_total(field, ammount)

        except sqlite3.OperationalError as e:
            raise e

        return 0

    def adjust_total(self) -> dict[str, str | dict]:
        """
        Comprueba que los datos de la tabla `total` está al día con los datos de la tabla `blockchain`.
        """

        total = self.get_df_total()

        cursor = self.cursor()

        try:
            results = {}

            for fuente in self.fields.keys():

                row = total[
                    total["fuente"] == (self.__TOTAL_TABLE["prefix"] + fuente)
                ].iloc[0]

                present_ammount = row["cantidad"]
                real_ammount = self.get_sum_of(fuente)

                if present_ammount == real_ammount:
                    results[fuente] = "OK"

                else:
                    cursor.execute(
                        "UPDATE {table} SET cantidad={cantidad} WHERE id={id}".format(
                            table=self.__TOTAL_TABLE["name"],
                            cantidad=real_ammount,
                            id=row["id"],
                        )
                    )

                    results[fuente] = {
                        "old": present_ammount,
                        "new": real_ammount,
                    }

        except sqlite3.OperationalError as e:
            raise e

        finally:
            cursor.close()

        return results

    def __is_valid(self, fuente: str, accept_t: bool = True) -> bool:
        """
        Comprueba si la fuente <fuente> es válida (es decir, se encuentra en la configuración)

        accept_t:    Devolver True si <fuente> está dentro de los valores: ["*", "t", ""]
        """
        return (
            fuente in self.fields.keys()
            or fuente in self.fields_.keys()
            or (fuente in ["*", "t", ""] if accept_t else False)
        )

    def get_sum_of(self, fuente: str, ndigits: int = 2) -> float:
        """
        Devuelve la suma del capital de todos los registros que coincidan con la fuente <fuente>

        fuente:    la fuente por la cual filtrar
        ndigits:   número de digitos después de la coma a mostrar
        """
        assert self.__is_valid(fuente, accept_t=False)
        assert isinstance(ndigits, int)

        matching = self.get_df_blockchain(fuente)["cantidad"]

        return round(matching.sum(), ndigits)

    def __get_df(self, table_name: str, fuente: str = "") -> DataFrame:
        """
        Devuelve un DataFrame con la tabla <table_name>, y filtra por el criterio <fuente>

        table_name:    nombre de la tabla que retornar
        fuente:        filtrar por la fuente <fuente>
        """
        assert isinstance(table_name, str)
        assert self.__is_valid(fuente)

        query = "SELECT * FROM %s" % table_name

        if fuente in self.fields.keys():
            query += " WHERE fuente='%s'" % fuente.upper()

        elif fuente in self.fields_.keys():
            query += " WHERE fuente IN %s" % str(self.fields_[fuente]).replace(
                "{", "("
            ).replace("}", ")")

        result = read_sql(query, self)

        return result

    def get_df_blockchain(self, fuente: str = "") -> DataFrame:
        """
        Devuelve un DataFrame con la tabla `blockchain`

        fuente:    filtrar por la fuente <fuente>
        """
        return self.__get_df(self.__MAIN_TABLE["name"], fuente)

    def get_df_total(self, fuente: str = "") -> DataFrame:
        """
        Devuelve un DataFrame con la tabla `total`

        fuente:    filtrar por la fuente <fuente>
        """
        return self.__get_df(self.__TOTAL_TABLE["name"], fuente)

    def create_local_backup(self, verbose: bool = True) -> str:
        """
        Crea una copia de seguridad de la base de datos y devuelve la ruta.

        verbose:    modo verboso
        """

        backup_folder = "%(backups_folder)sbackup %(datetime)s/" % {
            "backups_folder": self.backups_folder,
            "datetime": "".join(
                list(map(lambda char: "" if char in "- :" else char, get_time_now()))
            ),
        }

        makedirs(backup_folder, exist_ok=True)

        backup_route = backup_folder + (
            self.db_name + ".db" if not self.db_name.endswith(".db") else ""
        )

        backup = sqlite3.Connection(backup_route)

        self.backup(backup)

        backup.close()

        if verbose:
            print("Backup creado en:  %s" % (backup_route))

        return backup_route

    def get_latest_local_backup(self, delete_old_backups: bool = False) -> str:
        """
        Devuelve la ruta de la base de datos local más reciente.

        delete_old_backups:    eliminar todas las copias de seguridad anteriores a la última
        """

        carpetas = [
            folder
            for folder in listdir(self.backups_folder)
            if folder.startswith("backup")
        ]

        if len(carpetas) == 0:
            return "EMPTY"

        carpetas.sort(reverse=True)

        ultima_carpeta = carpetas.pop(0)

        if delete_old_backups:
            from shutil import rmtree

            for carpeta_vieja in carpetas:
                rmtree(
                    "%(backups_folder)s%(old_backup)s/"
                    % {
                        "backups_folder": self.backups_folder,
                        "old_backup": carpeta_vieja,
                    }
                )

        return "%(backups_folder)s%(latest_backup)s/" % {
            "backups_folder": self.backups_folder,
            "latest_backup": ultima_carpeta,
        }

    def restore_local_backup(
        self,
        delete_used_backup: bool = False,
        delete_old_backups: bool = False,
        create_backup: bool = True,
        verbose: bool = True,
    ) -> int:
        """
        Restaura una copia de seguridad si es que existe.

        delete_used_backup:     eliminar el backup usado
        delete_old_backups:     eliminar todas las copias de seguridad anteriores a la última
        create_backup:          crear un backup con el estado actual de la base de datos
        verbose:                modo verboso
        """

        latest_backup = self.get_latest_local_backup(delete_old_backups)

        if latest_backup == "EMPTY":
            if verbose:
                print("No existe ningún backup para recuperar.")
            return 1

        if create_backup:
            self.create_local_backup()

        latest_backup_route = (
            latest_backup + self.db_name + ".db"
            if not self.db_name.endswith(".db")
            else ""
        )

        backup = sqlite3.Connection(latest_backup_route)

        backup.backup(self)

        backup.close()

        if verbose:
            print("Backup restaurado: %s" % latest_backup_route)

        if delete_used_backup:
            remove(latest_backup_route)
            rmdir(latest_backup)

        return 0

    def get_capital_variation(self, activo: str):
        """
        Devuelve un DataFrame con la fecha y el capital al final del día en esa fecha.

        activo:         Activo a analizar. Debe estar dentro de los valores del diccionario self.fields
        """

        activo = activo.upper()

        if not (activo in self.fields.keys() or activo in self.fields.values()):
            return None

        df = self.get_df_blockchain()

        df = df[
            df.apply(
                lambda x: (
                    (x["fuente"] in self.fields_[activo])
                    if activo in self.fields_.keys()
                    else (x["fuente"] == activo)
                ),
                1,
            )
        ]

        def get_date(iso_datetime):
            return iso_datetime.split()[0]

        days = dict.fromkeys([get_date(datetime) for datetime in df["hora"].values], 0)

        def fn(row):
            days[get_date(row["hora"])] += row["cantidad"]

        df.apply(fn, 1)

        total = 0

        for day in days:
            total += days[day]
            days[day] = total

        figure = lineplot(days)
        figure.minorticks_on()
        figure.yaxis.set_major_formatter("{x:.2f}")

        figure.set_xmargin(-0.3)

        pyplot.show()

    def move_data(
        self,
        initial_id: int,
        include_initial_id: bool = True,
        ammount_to_move: int = 1,
        create_backup: bool = True,
    ):
        """
        Mueve el registro identificado con el argumento <initial_id> tantas veces como diga el argumento <amount_to_move>
        Si se especifica el argumento <include_initial_id> como True, el registro con el id <initial_id> será incluido en el desplazamiento, si no, comenzará una posición adelante.

        """

        assert ammount_to_move != 0

        if create_backup:
            self.create_local_backup()

        df = self.get_df_blockchain()

        if initial_id not in df["id"].values:
            print(
                "Id %(initial_id)s no encontrado en la tabla %(table)s"
                % {"initial_id": initial_id, "table": self.__MAIN_TABLE["name"]}
            )
            return False

        cursor = self.cursor()

        try:
            for i in (
                range(len(df) - 1, -1, -1) if ammount_to_move > 0 else range(len(df))
            ):
                row = df.iloc[i]
                old_id = row["id"]

                if old_id >= initial_id if include_initial_id else old_id > initial_id:
                    new_id = row["id"] + ammount_to_move

                    try:
                        cursor.execute(
                            "UPDATE %(table)s SET id=%(new_id)s WHERE id=%(old_id)s"
                            % {
                                "table": self.__MAIN_TABLE["name"],
                                "new_id": new_id,
                                "old_id": old_id,
                            }
                        )
                    except sqlite3.IntegrityError:
                        print(
                            "Error al tratar de cambiar el id %(old_id)s al id %(new_id)s. El id %(old_id)s no puede existir más de una vez!"
                            % {"new_id": new_id, "old_id": old_id},
                        )
                        return False

        except sqlite3.OperationalError as e:
            raise e

        finally:
            cursor.close()

        return True

    def __invert_fields(self, fields: dict) -> dict:
        """
        Invierte el orden de los pares clave:valor de un diccionario. Si se repiten los valores, estos se almacenarán en una lista.
        """

        campos_ = {value: set() for value in fields.values()}

        for key, value in fields.items():
            campos_[value].add(key)

        return campos_
