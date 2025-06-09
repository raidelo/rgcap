from pandas import read_sql

from .functions import clear_screen, clinput, float_from_str
from .rg_controller import RGController


def mainloop(controller: RGController) -> None:
    while True:
        cmd = clinput(
            "Ingresa una opción (número, %s): "
            % ", ".join(
                [
                    "'%s'" % field.lower()
                    for field in list(
                        filter(
                            lambda key: key if key != controller.fields else "",
                            controller.fields.keys(),
                        )
                    )
                ]
            )
        ).lower()

        if cmd == "q":
            # Salir del programa.

            break

        elif cmd in ["cls", "clear"]:
            # Limpiar la pantalla.

            clear_screen()

            continue

        cmd_list = cmd.split()
        cmd_len = len(cmd_list)

        number = float_from_str(cmd)

        if number:
            # Ingresar una cantidad de efectivo en la base de datos en el campo `default_field`.

            description = input("Descripción? ")

            res = controller.update_db(controller.default_field, number, description)

            if res == 0:
                print("OK")
            elif res == 1:
                print("ERROR")

        elif cmd.upper() in controller.fields:
            # Ingresar una cantidad de efectivo en la base de datos en el campo especificado.

            e = clinput("-> ")

            if e == "q":
                continue

            number = float_from_str(e)

            if number:
                description = input("Descripción? ")

                res = controller.update_db(cmd, number, description)

                if res == 0:
                    print("OK")
                elif res == 1:
                    print("ERROR")
            else:
                print("ERROR")

        elif cmd == "t":
            # Obtener el total de algo.

            cmd = clinput("-> ").upper()

            if cmd == "q":
                continue

            try:
                result = controller.get_sum_of(cmd)

                print("{}: {:.2f}".format(cmd, result))

            except AssertionError:
                print("Dato incorrecto.")

        elif cmd in ["txt", "savetxt"]:
            # Obtener toda la base de datos "total".

            content_to_write = controller.get_df_total().to_string(index=False) + "\n\n"

            for active_type in controller.fields_.keys():
                content_to_write += "Total {}: {:.2f}\n".format(
                    active_type, controller.get_sum_of(active_type)
                )

            print(content_to_write.strip())

            if cmd == "savetxt":
                ruta = "{}/{}.txt".format(controller.base_folder, controller.db_name)

                with open(ruta, "w") as file:
                    file.write(content_to_write)

                print("Guardado en {}".format(ruta))

        elif cmd in [
            "txt_t",
            "txt_b",
            "txt-t",
            "txt-b",
            "print_all",
            "printall",
            "print_b",
            "printb",
        ]:
            # Obtener toda la base de datos "blockchain".

            cmd = clinput("-> ").upper()

            if cmd == "q":
                continue

            try:
                print(controller.get_df_blockchain(cmd).to_string())
            except AssertionError:
                print("Dato incorrecto.")

        elif cmd in ["savecsv", "save_csv", "csv"]:
            # Obtener la tabla especificada en formato .csv. Por defecto "blockchain"

            tables = {
                "1": "blockchain",
                "2": "total",
            }

            cmd = cmd_list

            answer = clinput("[1] -> blockchain\n[2] -> total\n->")

            if answer in tables.keys():
                table = tables[answer]

            elif answer in tables.values():
                table = answer

            elif answer == "":
                table = "blockchain"

            else:
                print("Dato incorrecto.\n")
                continue

            if table in ["1", "blockchain"]:
                df = controller.get_df_blockchain()

            elif table in ["2", "total"]:
                df = controller.get_df_total()

            df.to_csv("{}/{}.csv".format(controller.base_folder, table), index=False)

            print(df.to_string())

            print("Archivo `.csv` guardado con éxito.")

        elif cmd in ["stat", "statistics", "estadisticas", "estadísticas"] or (
            cmd_len == 2
            and cmd_list[0] in ["stat", "statistics", "estadisticas", "estadísticas"]
        ):
            # Obtener una gráfica con estadísticas sobre un activo determinado.

            if cmd_len == 2:
                activo = cmd_list[1]
            else:
                activo = clinput("-> ")

            if activo == "":
                activo = controller.default_field

            try:
                controller.get_capital_variation(activo)

            except ModuleNotFoundError as e:
                print("error: herramienta `%s` no instalada" % e.name)

        elif cmd in [
            "sql",
            "sql_query",
            "sqlquery",
            "sqlite",
            "sqlite_query",
            "sqlitequery",
            "query",
        ]:
            # Poder interactuar directamente con la base de datos a través del lenguaje SQL.

            while True:
                e = input("-> ")

                if e == "q":
                    break

                elif e.startswith("select"):
                    try:
                        print(e)

                        df = read_sql(e, controller).to_string()

                        print(df)

                    except BaseException as e:
                        print(e)

                else:
                    cursor = controller.cursor()

                    try:
                        if e.startswith("delete") or e.startswith("update"):
                            controller.create_local_backup()

                        cursor.execute(e)

                    except BaseException as e:
                        print(e)

                    finally:
                        cursor.close()

        elif cmd.lower() in [
            "savebackup",
            "save_backup",
            "createbackup",
            "create_backup",
        ]:
            # Crear una copia de seguridad local de la base de datos entera.

            controller.create_local_backup()

        elif cmd in [
            "recoverbackup",
            "recover_backup",
            "restorebackup",
            "restore_backup",
        ]:
            # Restablecer base de datos local con una copia de seguridad local.

            controller.restore_local_backup(delete_used_backup=True)

        elif cmd in ["adjust", "rectify"]:
            # Rectificar tabla total.

            results = controller.adjust_total()

            for fuente in results:
                if results[fuente] == "OK":
                    print("OK {}".format(fuente))

                else:
                    print(
                        "ACTUALIZED {fuente}   ( {old} -> {new} )".format(
                            fuente=fuente,
                            old=results[fuente]["old"],
                            new=results[fuente]["new"],
                        )
                    )

        else:
            print("Dato incorrecto.")

        print()
