void showCLI() {
    if (!dataframe_activo) {
        print_error("No hay DataFrame cargado.");
        return;
    }

    // Mostrar el nombre del DataFrame
    printf(WHITE "                    DataFrame: %s\n\n" RESET, dataframe_activo->indice);
    
    // Imprimir encabezados
    for (int col = 0; col < dataframe_activo->numColumnas; col++) {
        printf("%-20s", dataframe_activo->columnas[col].nombre);
    }
    printf("\n");

    
    for (int row = 0; row < dataframe_activo->numFilas; row++) {
        for (int col = 0; col < dataframe_activo->numColumnas; col++) {
            if (dataframe_activo->columnas[col].esNulo[row]) {
                printf("%-20s", "1");  // Ajustar la longitud para mejor alineación
            } else {
                // Asegurarse de no intentar imprimir punteros NULL
                if (dataframe_activo->columnas[col].datos[row] == NULL) {
                    printf("%-20s", "");  // En caso de que el valor sea NULL, imprimir "NULL"
                } else {
                    printf("%-20s", (char *)dataframe_activo->columnas[col].datos[row]);
                }
            }
        }
        printf("\n");
    }
}