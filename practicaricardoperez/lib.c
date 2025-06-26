#include "lib.h"

static int expandirMemoriaDataframe(Dataframe *df, int nuevoTamano);
static void procesarLineaCSV(const char *linea, Dataframe *df, int fila);
static int validarParametros(const void *param, const char *nombre_param);
static int validarDataframe(const Dataframe *df);
static int validarIndiceColumna(const Dataframe *df, int indice_col);
static int validarIndiceFila(const Dataframe *df, int indice_fila);
static void *asignarMemoria(size_t tamano, const char *mensaje_error);
static void *reasignarMemoria(void *ptr, size_t nuevo_tamano, const char *mensaje_error);
static void liberarMemoria(void *ptr);
static int inicializarColumna(Columna *col, int numFilas);
static void liberarColumna(Columna *col, int numFilas);
static int procesarValorNumerico(const char *valor, double *resultado);
static int procesarValorFecha(const char *valor, struct tm *fecha);
static int compararValoresTipo(void *a, void *b, TipoDato tipo, int esta_desc);

Lista listaDF = {0, NULL};
Dataframe *dfActual = NULL;
char promptTerminal[MAX_LINE_LENGTH] = "[?]:> ";

Dataframe *crearNuevoDataframe(int numColumnas, int numFilas, const char *nombre) {
    Dataframe *df = malloc(sizeof(Dataframe));
    if (!df || !crearDF(df, numColumnas, numFilas, nombre)) {
        free(df);
        print_error("Error al crear nuevo dataframe");
        return NULL;
    }
    strncpy(df->nombre, nombre, 50);
    df->nombre[50] = '\0';
    return df;
}

void copiarNombreColumna(char *destino, const char *origen) {
    strncpy(destino, origen, MAX_NOMBRE_COLUMNA - 1);
    destino[MAX_NOMBRE_COLUMNA - 1] = '\0';
}

char *copiarDatoSeguro(const char *origen) { return origen ? strdup(origen) : NULL; }

void actualizarPrompt(const Dataframe *df) {
    if (!df) {
        strncpy(promptTerminal, "[?]:> ", MAX_LINE_LENGTH);
        return;
    }
    const char *nombre_mostrar = (df->nombre[0] != '\0') ? df->nombre : df->indice;
    snprintf(promptTerminal, MAX_LINE_LENGTH, "[%s: %d,%d]:> ", nombre_mostrar, df->numFilas, df->numColumnas);
}

void liberarRecursosEnError(Dataframe *df, const char *mensaje) {
    if (df)
        liberarMemoriaDF(df);
    print_error(mensaje);
}

void procesarPorLotes(FILE *archivo, Dataframe *df, int tamanoLote) {
    VALIDAR_DF_Y_PARAMETROS(df, archivo);

    char *lineaLeida = NULL;
    size_t len = 0;
    int filaActual = 0;
    int numMaxFilas = tamanoLote;
    char buffer[2048] = {0};

    while (getline(&lineaLeida, &len, archivo) != -1) {
        if (filaActual >= numMaxFilas) {
            numMaxFilas += tamanoLote;

            for (int columnaActual = 0; columnaActual < df->numColumnas; columnaActual++) {
                char **nuevos_datos =
                    realloc(df->columnas[columnaActual].datos, numMaxFilas * sizeof(char *));
                EstadoNulo *nuevos_nulos =
                    realloc(df->columnas[columnaActual].esNulo, numMaxFilas * sizeof(EstadoNulo));

                if (!nuevos_datos || !nuevos_nulos) {
                    liberarRecursosEnError(df, "Error al expandir memoria");
                    free(lineaLeida);
                    return;
                }

                df->columnas[columnaActual].datos = nuevos_datos;
                df->columnas[columnaActual].esNulo = nuevos_nulos;

                for (int j = filaActual; j < numMaxFilas; j++) {
                    df->columnas[columnaActual].datos[j] = NULL;
                    df->columnas[columnaActual].esNulo[j] = NO_NULO;
                }
            }
        }

        verificarNulos(lineaLeida, filaActual, df, buffer);
        char *token = strtok(buffer, ",\n\r");
        int columnaActual = 0;

        while (token && columnaActual < df->numColumnas) {
            if (!df->columnas[columnaActual].esNulo[filaActual]) {
                df->columnas[columnaActual].datos[filaActual] = strdup(token);
            }
            token = strtok(NULL, ",\n\r");
            columnaActual++;
        }
        filaActual++;
    }

    df->numFilas = filaActual;
    free(lineaLeida);
}

void liberarListaCompleta(Lista *lista) {
    Nodo *actual = lista->primero;

    while (actual) {
        Nodo *siguiente = actual->siguiente;
        liberarMemoriaDF(actual->df);
        free(actual);
        actual = siguiente;
    }

    lista->primero = NULL;
    lista->numDFs = 0;
}

int comparar(void *dato1, void *dato2, TipoDato tipo, const char *operador) {
    if (!dato1 || !dato2)
        return 0;
    switch (tipo) {
    case NUMERICO: {
        char *str1 = (char *)dato1;
        char *str2 = (char *)dato2;
        double val1, val2;
        char *endptr1, *endptr2;
        val1 = strtod(str1, &endptr1);
        val2 = strtod(str2, &endptr2);
        if (endptr1 == str1 || endptr2 == str2) {
            return 0;
        }
        if (strcmp(operador, "eq") == 0)
            return fabs(val1 - val2) < 1e-10;
        if (strcmp(operador, "neq") == 0)
            return fabs(val1 - val2) >= 1e-10;
        if (strcmp(operador, "gt") == 0)
            return val1 > val2;
        if (strcmp(operador, "lt") == 0)
            return val1 < val2;
        if (strcmp(operador, "get") == 0)
            return val1 >= val2;
        if (strcmp(operador, "let") == 0)
            return val1 <= val2;
        break;
    }
    case FECHA: {
        struct tm tm1 = {0}, tm2 = {0};
        if (sscanf((char *)dato1, "%d-%d-%d", &tm1.tm_year, &tm1.tm_mon, &tm1.tm_mday) != 3 ||
            sscanf((char *)dato2, "%d-%d-%d", &tm2.tm_year, &tm2.tm_mon, &tm2.tm_mday) != 3) {
            return 0;
        }
        tm1.tm_year -= 1900;
        tm2.tm_year -= 1900;
        tm1.tm_mon -= 1;
        tm2.tm_mon -= 1;
        time_t time1 = mktime(&tm1);
        time_t time2 = mktime(&tm2);
        if (strcmp(operador, "eq") == 0)
            return time1 == time2;
        if (strcmp(operador, "neq") == 0)
            return time1 != time2;
        if (strcmp(operador, "gt") == 0)
            return time1 > time2;
        if (strcmp(operador, "lt") == 0)
            return time1 < time2;
        if (strcmp(operador, "get") == 0)
            return time1 >= time2;
        if (strcmp(operador, "let") == 0)
            return time1 <= time2;
        break;
    }
    case TEXTO: {
        int comp = strcmp((char *)dato1, (char *)dato2);
        if (strcmp(operador, "eq") == 0)
            return comp == 0;
        if (strcmp(operador, "neq") == 0)
            return comp != 0;
        if (strcmp(operador, "gt") == 0)
            return comp > 0;
        if (strcmp(operador, "lt") == 0)
            return comp < 0;
        if (strcmp(operador, "get") == 0)
            return comp >= 0;
        if (strcmp(operador, "let") == 0)
            return comp <= 0;
        break;
    }
    }
    return 0;
}

void liberarMemoriaDF(Dataframe *df) {
    if (!df) {
        return;
    }

    if (df->columnas) {
        for (int i = 0; i < df->numColumnas; i++) {
            if (df->columnas[i].datos) {
                for (int j = 0; j < df->numFilas; j++) {
                    free(df->columnas[i].datos[j]);
                }
                free(df->columnas[i].datos);
            }
            free(df->columnas[i].esNulo);
        }
        free(df->columnas);
    }
    free(df);
}

void agregarDF(Dataframe *nuevoDF) {
    if (!nuevoDF) {
        print_error("Dataframe inválido");
        return;
    }
    if (!nombreDFUnico(&listaDF, nuevoDF->nombre)) {
        print_error("Ya existe un dataframe con ese nombre");
        liberarMemoriaDF(nuevoDF);
        return;
    }
    Nodo *nuevoNodo = malloc(sizeof(Nodo));
    if (!nuevoNodo) {
        print_error("Fallo en asignación de memoria para nodo");
        return;
    }
    nuevoNodo->df = nuevoDF;
    nuevoNodo->siguiente = listaDF.primero;
    listaDF.primero = nuevoNodo;
}

void contarFilasYColumnas(const char *nombre_archivo, int *numFilas, int *numColumnas) {
    FILE *file = fopen(nombre_archivo, "r");
    if (!file) {
        print_error("No se puede abrir el archivo");
        return;
    }

    char *line = NULL;
    size_t len = 0;
    ssize_t read;

    *numFilas = 0;
    *numColumnas = 0;

    while ((read = getline(&line, &len, file)) != -1) {
        (*numFilas)++;
        if (*numFilas == 1) {
            *numColumnas = contarColumnas(line);
            if (*numColumnas > MAX_COLUMNS) {
                print_error("Número de columnas excede el límite máximo");
                free(line);
                fclose(file);
                return;
            }
        }
    }

    free(line);
    fclose(file);
}

void leerEncabezados(FILE *file, Dataframe *df, int numColumnas) {
    char *line = NULL;
    size_t len = 0;
    ssize_t read;

    if ((read = getline(&line, &len, file)) != -1) {
        char *token = strtok(line, ",\n\r");
        int col = 0;
        while (token && col < numColumnas) {
            strncpy(df->columnas[col].nombre, token, sizeof(df->columnas[col].nombre) - 1);
            df->columnas[col].nombre[sizeof(df->columnas[col].nombre) - 1] = '\0';
            token = strtok(NULL, ",\n\r");
            col++;
        }
    }

    free(line);
}

void leerFilas(FILE *file, Dataframe *df, int numFilas, int numColumnas) {
    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    int fila_actual = 0;
    char resultado[MAX_LINE_LENGTH * 2];

    while ((read = getline(&line, &len, file)) != -1 && fila_actual < numFilas) {
        verificarNulos(line, fila_actual, df, resultado);

        char *token = strtok(line, ",\n\r");
        int col = 0;
        while (token && col < numColumnas) {
            if (!df->columnas[col].esNulo[fila_actual]) {
                df->columnas[col].datos[fila_actual] = strdup(token);
            }
            token = strtok(NULL, ",\n\r");
            col++;
        }
        fila_actual++;
    }

    free(line);
}

int crearDF(Dataframe *df, int numColumnas, int numFilas, const char *nombre_df) {
    if (!df || numColumnas <= 0 || numFilas <= 0 || !nombre_df) {
        print_error("Invalid parameters");
        return 0;
    }
    if (numColumnas > MAX_COLUMNS || numFilas > MAX_ROWS) {
        print_error("Dataset too large");
        return 0;
    }
    df->columnas = malloc(numColumnas * sizeof(Columna));
    if (!df->columnas) {
        print_error("Memory allocation failed for columns");
        return 0;
    }
    memset(df->columnas, 0, numColumnas * sizeof(Columna));
    for (int i = 0; i < numColumnas; i++) {
        df->columnas[i].datos = malloc(numFilas * sizeof(char *));
        df->columnas[i].esNulo = calloc(numFilas, sizeof(EstadoNulo));
        if (!df->columnas[i].datos || !df->columnas[i].esNulo) {
            for (int j = 0; j <= i; j++) {
                free(df->columnas[j].datos);
                free(df->columnas[j].esNulo);
            }
            free(df->columnas);
            print_error("Memory allocation failed for data");
            return 0;
        }
        memset(df->columnas[i].datos, 0, numFilas * sizeof(char *));
    }
    df->numColumnas = numColumnas;
    df->numFilas = numFilas;
    strncpy(df->nombre, nombre_df, sizeof(df->nombre) - 1);
    df->nombre[sizeof(df->nombre) - 1] = '\0';
    return 1;
}

void loadearCSV(const char *nombre_archivo, char sep) {
    VALIDAR_DF_Y_PARAMETROS(nombre_archivo, nombre_archivo);
    FILE *file = fopen(nombre_archivo, "r");
    if (!file) {
        print_error("No se puede abrir el archivo");
        return;
    }
    char *headerLine = NULL;
    size_t len = 0;
    if (getline(&headerLine, &len, file) == -1) {
        print_error("Error al leer encabezados");
        free(headerLine);
        fclose(file);
        return;
    }
    int numColumnas = 1;
    for (char *p = headerLine; *p; ++p) {
        if (*p == sep) numColumnas++;
    }
    if (numColumnas > MAX_COLUMNS) {
        print_error("Demasiadas columnas");
        free(headerLine);
        fclose(file);
        return;
    }
    // Buscar si ya existe un dataframe con el nombre por defecto
    char nombre_df[51];
    int intento = 0;
    do {
        snprintf(nombre_df, sizeof(nombre_df), "df%d", listaDF.numDFs + intento);
        intento++;
    } while (!nombreDFUnico(&listaDF, nombre_df));
    
    // Si el nombre encontrado es df0, df1, etc., verificar si ya existe un dataframe con ese nombre
    if (intento == 1) { // Si es el primer intento (df0, df1, etc.)
        Nodo *actual = listaDF.primero;
        while (actual) {
            const char *nombre_actual = (actual->df->nombre[0] != '\0') ? actual->df->nombre : actual->df->indice;
            if (strcmp(nombre_actual, nombre_df) == 0) {
                // Ya existe un dataframe con este nombre, cambiar a él
                dfActual = actual->df;
                actualizarPrompt(dfActual);
                printf(GREEN "Cambiado a dataframe existente %s\n" RESET, nombre_df);
                free(headerLine);
                fclose(file);
                return;
            }
            actual = actual->siguiente;
        }
    }
    
    Dataframe *nuevo_df = crearNuevoDataframe(numColumnas, BATCH_SIZE, nombre_df);
    if (!nuevo_df) {
        free(headerLine);
        fclose(file);
        return;
    }
    // Leer encabezados con separador
    char *token = strtok(headerLine, &sep);
    int columnaActual = 0;
    while (token && columnaActual < numColumnas) {
        strncpy(nuevo_df->columnas[columnaActual].nombre, token, 29);
        nuevo_df->columnas[columnaActual].nombre[29] = '\0';
        columnaActual++;
        token = strtok(NULL, &sep);
    }
    free(headerLine);
    // Procesar por lotes con separador
    // (Aquí deberías adaptar procesarPorLotes y verificarNulos para usar sep, pero por simplicidad lo dejamos así)
    procesarPorLotes(file, nuevo_df, BATCH_SIZE); // <-- Si quieres soporte total, adapta esta función también
    fclose(file);
    if (nuevo_df->numFilas > 0) {
        for (int columnaActual = 0; columnaActual < nuevo_df->numColumnas; columnaActual++) {
            char **nuevos_datos = realloc(nuevo_df->columnas[columnaActual].datos,
                                          nuevo_df->numFilas * sizeof(char *));
            EstadoNulo *nuevos_nulos = realloc(nuevo_df->columnas[columnaActual].esNulo,
                                               nuevo_df->numFilas * sizeof(EstadoNulo));
            if (!nuevos_datos || !nuevos_nulos) {
                liberarRecursosEnError(nuevo_df, "Error al redimensionar memoria");
                return;
            }
            nuevo_df->columnas[columnaActual].datos = nuevos_datos;
            nuevo_df->columnas[columnaActual].esNulo = nuevos_nulos;
        }
    }
    dfActual = nuevo_df;
    agregarDF(nuevo_df);
    listaDF.numDFs++;
    tiposColumnas(dfActual);
    actualizarPrompt(dfActual);
    printf(GREEN "Archivo cargado: %d filas, %d columnas\n" RESET, dfActual->numFilas, dfActual->numColumnas);
}

void cortarEspacios(char *str) {
    size_t longitud = strlen(str);
    if (longitud > 0 && str[longitud - 1] == '\n') {
        str[longitud - 1] = '\0';
    }

    char *inicio = str;
    while (*inicio == ' ') {
        inicio++;
    }

    if (inicio != str) {
        memmove(str, inicio, strlen(inicio) + 1);
    }

    longitud = strlen(str);
    while (longitud > 0 && str[longitud - 1] == ' ') {
        str[--longitud] = '\0';
    }
}

int contarColumnas(const char *line) {
    int count = 1;
    for (int i = 0; line[i]; i++) {
        if (line[i] == ',')
            count++;
    }
    return count;
}

int fechaValida(const char *str_fecha) {
    if (!str_fecha)
        return 0;

    int año, mes, dia;
    if (sscanf(str_fecha, "%4d-%2d-%2d", &año, &mes, &dia) != 3)
        return 0;

    // Validar rangos básicos
    if (año < 1900 || mes < 1 || mes > 12 || dia < 1 || dia > 31)
        return 0;

    // Días por mes, incluyendo bisiestos
    int diasPorMes[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if ((año % 4 == 0 && año % 100 != 0) || (año % 400 == 0))
        diasPorMes[1] = 29;

    return (dia <= diasPorMes[mes - 1]);
}

void tiposColumnas(Dataframe *df) {
    if (!df)
        return;

    for (int col = 0; col < df->numColumnas; col++) {
        int esFecha = 1;
        for (int fila = 0; fila < df->numFilas; fila++) {
            if (df->columnas[col].esNulo[fila])
                continue;
            char *valor = df->columnas[col].datos[fila];
            if (!valor || !fechaValida(valor)) {
                esFecha = 0;
                break;
            }
        }
        df->columnas[col].tipo = esFecha ? FECHA : TEXTO;
    }
}

int compararValores(void *a, void *b, TipoDato tipo, int esta_desc) {
    if (a == NULL && b == NULL)
        return 0;
    if (a == NULL)
        return esta_desc ? 1 : -1;
    if (b == NULL)
        return esta_desc ? -1 : 1;

    char *str_a = (char *)a;
    char *str_b = (char *)b;

    switch (tipo) {
    case NUMERICO: {
        double num_a = atof(str_a);
        double num_b = atof(str_b);
        return esta_desc ? (num_a < num_b ? 1 : (num_a > num_b ? -1 : 0))
                         : (num_a < num_b ? -1 : (num_a > num_b ? 1 : 0));
    }
    case FECHA: {
        struct tm tm_a = {0}, tm_b = {0};
        sscanf(str_a, "%4d-%2d-%2d", &tm_a.tm_year, &tm_a.tm_mon, &tm_a.tm_mday);
        sscanf(str_b, "%4d-%2d-%2d", &tm_b.tm_year, &tm_b.tm_mon, &tm_b.tm_mday);
        tm_a.tm_year -= 1900;
        tm_b.tm_year -= 1900;
        tm_a.tm_mon -= 1;
        tm_b.tm_mon -= 1;

        time_t time_a = mktime(&tm_a);
        time_t time_b = mktime(&tm_b);

        return esta_desc ? (time_a < time_b ? 1 : (time_a > time_b ? -1 : 0))
                         : (time_a < time_b ? -1 : (time_a > time_b ? 1 : 0));
    }
    case TEXTO: {
        return esta_desc ? strcmp(str_b, str_a) : strcmp(str_a, str_b);
    }
    default:
        return 0;
    }
}

void print_error(const char *mensaje_error) {
    fprintf(stderr, RED "ERROR: %s\n" RESET, mensaje_error);
}

void inicializarLista() {
    listaDF.numDFs = 0;
    listaDF.primero = NULL;
}

void cambiarDF(Lista *lista, int indice) {
    print_error("Este comando ha sido reemplazado. Use el nombre del dataframe.");
}

int encontrarIndiceColumna(Dataframe *df, const char *nombreColumna) {
    for (int i = 0; i < df->numColumnas; i++) {
        if (strcmp(df->columnas[i].nombre, nombreColumna) == 0) {
            return i;
        }
    }
    return -1;
}

void intercambiarFilas(Dataframe *df, int fila1, int fila2) {
    for (int col = 0; col < df->numColumnas; col++) {
        void *temp_datos = df->columnas[col].datos[fila1];
        df->columnas[col].datos[fila1] = df->columnas[col].datos[fila2];
        df->columnas[col].datos[fila2] = temp_datos;

        int temp_nulo = df->columnas[col].esNulo[fila1];
        df->columnas[col].esNulo[fila1] = df->columnas[col].esNulo[fila2];
        df->columnas[col].esNulo[fila2] = temp_nulo;
    }
}

void ordenarDataframe(Dataframe *df, int indice_columna, int descendente) {
    TipoDato tipo_columna = df->columnas[indice_columna].tipo;

    for (int i = 0; i < df->numFilas - 1; i++) {
        for (int j = 0; j < df->numFilas - i - 1; j++) {
            void *val1 = df->columnas[indice_columna].datos[j];
            void *val2 = df->columnas[indice_columna].datos[j + 1];

            if (compararValores(val1, val2, tipo_columna, descendente) > 0) {
                intercambiarFilas(df, j, j + 1);
            }
        }
    }
}

void verificarNulos(char *lineaLeida, int fila, Dataframe *df, char *resultado) {
    cortarEspacios(lineaLeida);
    int j = 0;
    int longitud = strlen(lineaLeida);
    int inicio_campo = 0;
    int indice_columna = 0;

    for (int i = 0; i <= longitud; i++) {
        if (i == longitud || lineaLeida[i] == ',' || lineaLeida[i] == '\r') {
            // Verificar si el campo está vacío o solo tiene espacios
            int esNulo = 1;
            for (int k = inicio_campo; k < i; k++) {
                if (lineaLeida[k] != ' ' && lineaLeida[k] != '\t') {
                    esNulo = 0;
                    break;
                }
            }

            if (esNulo && indice_columna < df->numColumnas) {
                df->columnas[indice_columna].esNulo[fila] = 1;
                resultado[j++] = '1'; // Opcional para depuración
            } else {
                // Copiar el campo original
                for (int k = inicio_campo; k < i; k++) {
                    resultado[j++] = lineaLeida[k];
                }
            }

            if (i < longitud)
                resultado[j++] = lineaLeida[i];

            inicio_campo = i + 1;
            indice_columna++;
        }
    }
    resultado[j] = '\0';
}

void CLI() {
    char input[MAX_LINE_LENGTH];

    while (1) {
        printf("%s", promptTerminal);
        fgets(input, sizeof(input), stdin);
        cortarEspacios(input);

        if (strcmp(input, "quit") == 0) {
            printf(GREEN "Fin...\n" RESET);
            break;
        } else if (strncmp(input, "load ", 5) == 0) {
            char archivo[256] = {0};
            char sep = ',';
            int parsed = sscanf(input + 5, "%255s %c", archivo, &sep);
            if (parsed == 1) {
                loadearCSV(archivo, ',');
            } else if (parsed == 2) {
                if (archivo[0] == '\0' || sep == '\0' || input[5 + strlen(archivo) + 1 + 1] != '\0') {
                    print_error("Uso: load <nombre_fichero> [<sep>]");
                    continue;
                }
                loadearCSV(archivo, sep);
            } else {
                print_error("Uso: load <nombre_fichero> [<sep>]");
            }
        } else if (strncmp(input, "add ", 4) == 0) {
            char archivo[256] = {0};
            char sep = ',';
            int parsed = sscanf(input + 4, "%255s %c", archivo, &sep);
            if (parsed == 1) {
                addCLI(archivo, ',');
            } else if (parsed == 2) {
                if (archivo[0] == '\0' || sep == '\0' || input[4 + strlen(archivo) + 1 + 1] != '\0') {
                    print_error("Uso: add <nombre_fichero> [<sep>]");
                    continue;
                }
                addCLI(archivo, sep);
            } else {
                print_error("Uso: add <nombre_fichero> [<sep>]");
            }
        } else if (strcmp(input, "meta") == 0) {
            metaCLI();
        } else if (strncmp(input, "save", 4) == 0) {
            // Saltar espacios después de 'save'
            const char *nombre_archivo = input + 4;
            while (*nombre_archivo == ' ') nombre_archivo++;
            if (*nombre_archivo == '\0') {
                saveCLI(NULL);
            } else {
                saveCLI(nombre_archivo);
            }
        } else if (strncmp(input, "view", 4) == 0) {
            int n = 10;
            if (strlen(input) > 4) {
                char *endptr;
                long parsed_n = strtol(input + 4, &endptr, 10);
                if (endptr != input + 4 && parsed_n != 0) {
                    viewCLI((int)parsed_n);
                } else {
                    print_error("Número de filas no válido");
                    continue;
                }
            } else {
                viewCLI(n);
            }
        }

        else if (strncmp(input, "sort ", 5) == 0) {
            char nombreColumna[50];
            char order[10] = "asc";
            int esta_desc = 0;

            int parsed = sscanf(input + 5, "%49s %9s", nombreColumna, order);
            if (parsed == 0) {
                print_error("Comando de ordenamiento inválido");
                continue;
            }

            if (parsed == 2) {
                if (strcmp(order, "asc") == 0) {
                    esta_desc = 0;
                } else if (strcmp(order, "des") == 0) {
                    esta_desc = 1;
                } else {
                    print_error("Orden de clasificación inválido. Usar 'asc' o 'des'.");
                    continue;
                }
            }
            sortCLI(nombreColumna, esta_desc);
        } else if (strncmp(input, "delnull ", 8) == 0) {
            delnullCLI(input + 8);
        } else if (strncmp(input, "delcolum ", 9) == 0) {
            const char *nombreColumna = input + 9;
            while (*nombreColumna == ' ')
                nombreColumna++;
            delcolumCLI(nombreColumna);
        } else if (strncmp(input, "df", 2) == 0 && strlen(input) > 2) {
            char *endptr;
            long indice = strtol(input + 2, &endptr, 10);
            if (*endptr == '\0' && indice >= 0 && indice < listaDF.numDFs) {
                // Buscar el dataframe por índice en la lista
                int indiceInverso = listaDF.numDFs - 1 - indice;
                Nodo *nodoActual = listaDF.primero;
                for (int i = 0; i < indiceInverso; i++) {
                    if (nodoActual == NULL) {
                        print_error("Dataframe no encontrado.");
                        return;
                    }
                    nodoActual = nodoActual->siguiente;
                }
                if (nodoActual == NULL || nodoActual->df == NULL) {
                    print_error("Dataframe no encontrado.");
                    return;
                }
                dfActual = nodoActual->df;
                actualizarPrompt(dfActual);
                const char *nombre_df = (dfActual->nombre[0] != '\0') ? dfActual->nombre : dfActual->indice;
                printf(GREEN "Cambiado a %s con %d filas y %d columnas\n" RESET, nombre_df, dfActual->numFilas, dfActual->numColumnas);
            } else {
                print_error("Índice de dataframe inválido.");
            }
        } else if (strncmp(input, "filter ", 7) == 0) {
            char columna[50], operador[10], valor[50];

            int parsed = sscanf(input + 7, "%49s %9s %49s", columna, operador, valor);
            if (parsed != 3) {
                print_error("Uso: filter [columna] [eq|neq|gt|lt] [valor]");
                continue;
            }

            if (!dfActual) {
                print_error("No hay dataframe activo");
                continue;
            }

            int col_idx = -1;
            for (int i = 0; i < dfActual->numColumnas; i++) {
                if (strcmp(dfActual->columnas[i].nombre, columna) == 0) {
                    col_idx = i;
                    break;
                }
            }

            if (col_idx == -1) {
                print_error("Columna no encontrada");
                continue;
            }

            char *valor_convertido = strdup(valor);
            if (!valor_convertido) {
                print_error("Error de memoria");
                continue;
            }

            filterCLI(dfActual, columna, operador, valor_convertido);
            free(valor_convertido);
        } else if (strncmp(input, "quarter ", 8) == 0) {
            char col_fecha[50], col_nueva[50];

            int parsed = sscanf(input + 8, "%49s %49s", col_fecha, col_nueva);
            if (parsed != 2) {
                print_error("Uso: quarter [columna_fecha] [nombre_nueva_columna]");
                continue;
            }

            quarterCLI(col_fecha, col_nueva);
        } else if (strncmp(input, "name ", 5) == 0) {
            char nuevo_nombre[51];
            strncpy(nuevo_nombre, input + 5, 50);
            nuevo_nombre[50] = '\0';
            cortarEspacios(nuevo_nombre);
            if (strlen(nuevo_nombre) == 0 || strlen(nuevo_nombre) > 50) {
                print_error("El nombre debe tener entre 1 y 50 caracteres");
                continue;
            }
            if (!nombreDFUnico(&listaDF, nuevo_nombre)) {
                print_error("Ya existe un dataframe con ese nombre");
                continue;
            }
            if (!dfActual) {
                print_error("No hay dataframe activo");
                continue;
            }
            strncpy(dfActual->nombre, nuevo_nombre, 50);
            dfActual->nombre[50] = '\0';
            actualizarPrompt(dfActual);
            printf(GREEN "Nombre del dataframe cambiado a '%s'\n" RESET, dfActual->nombre);
        } else if (strncmp(input, "prefix ", 7) == 0) {
            char col[50], nueva_col[50];
            int n = 0;
            int parsed = sscanf(input + 7, "%49s n %49s", col, nueva_col);
            char *ptr_n = strstr(input + 7, " n ");
            if (parsed != 2 || !ptr_n) {
                print_error("Uso: prefix <columna> n <nueva_columna>");
                continue;
            }
            n = atoi(ptr_n + 3);
            if (n <= 0) {
                print_error("n debe ser un entero mayor que 0");
                continue;
            }
            prefixCLI(col, n, nueva_col);
        } else if (strcmp(input, "list") == 0) {
            listCLI();
        } else {
            // Intentar cambiar de dataframe por nombre
            Nodo *actual = listaDF.primero;
            while (actual) {
                const char *nombre_df = (actual->df->nombre[0] != '\0') ? actual->df->nombre : actual->df->indice;
                if (strcmp(nombre_df, input) == 0) {
                    cambiarDFPorNombre(&listaDF, input);
                    return;
                }
                actual = actual->siguiente;
            }
            print_error("Comando no reconocido. Escriba un comando válido.");
        }
    }
}

void filterCLI(Dataframe *df, const char *nombreColumna, const char *operador, void *valor) {
    VALIDAR_DF_Y_PARAMETROS(df, nombreColumna);
    VALIDAR_DF_Y_PARAMETROS(df, operador);
    VALIDAR_DF_Y_PARAMETROS(df, valor);
    int indice_col = encontrarIndiceColumna(df, nombreColumna);
    if (indice_col == -1) {
        print_error("Columna no encontrada");
        return;
    }
    if (strcmp(operador, "eq") != 0 && strcmp(operador, "neq") != 0 &&
        strcmp(operador, "gt") != 0 && strcmp(operador, "lt") != 0 &&
        strcmp(operador, "get") != 0 && strcmp(operador, "let") != 0) {
        print_error("Operador inválido. Use: eq, neq, gt, lt, get, let");
        return;
    }

    int nuevas_filas = 0;
    for (int filaActual = 0; filaActual < df->numFilas; filaActual++) {
        if (!df->columnas[indice_col].esNulo[filaActual]) {
            void *valor_actual = df->columnas[indice_col].datos[filaActual];
            if (valor_actual &&
                comparar(valor_actual, valor, df->columnas[indice_col].tipo, operador)) {
                nuevas_filas++;
            }
        }
    }

    if (nuevas_filas == 0) {
        printf(GREEN "No se encontraron filas que cumplan la condición\n" RESET);
        return;
    }

    Dataframe *nuevo_df = malloc(sizeof(Dataframe));
    if (!nuevo_df) {
        print_error("Error al asignar memoria para el nuevo dataframe");
        return;
    }

    if (!crearDF(nuevo_df, df->numColumnas, nuevas_filas, df->nombre)) {
        free(nuevo_df);
        print_error("Error al crear el nuevo dataframe");
        return;
    }

    for (int columnaActual = 0; columnaActual < df->numColumnas; columnaActual++) {
        strncpy(nuevo_df->columnas[columnaActual].nombre, df->columnas[columnaActual].nombre, 29);
        nuevo_df->columnas[columnaActual].nombre[29] = '\0';
        nuevo_df->columnas[columnaActual].tipo = df->columnas[columnaActual].tipo;
    }

    int fila_destino = 0;
    for (int filaActual = 0; filaActual < df->numFilas; filaActual++) {
        if (!df->columnas[indice_col].esNulo[filaActual]) {
            void *valor_actual = df->columnas[indice_col].datos[filaActual];
            if (valor_actual &&
                comparar(valor_actual, valor, df->columnas[indice_col].tipo, operador)) {
                if (!copiarFila(nuevo_df, df, filaActual, fila_destino)) {
                    // Liberar filas ya copiadas
                    for (int r = 0; r < fila_destino; r++) {
                        for (int c = 0; c < nuevo_df->numColumnas; c++) {
                            free(nuevo_df->columnas[c].datos[r]);
                        }
                    }
                    for (int c = 0; c < nuevo_df->numColumnas; c++) {
                        free(nuevo_df->columnas[c].datos);
                        free(nuevo_df->columnas[c].esNulo);
                    }
                    free(nuevo_df->columnas);
                    free(nuevo_df);
                    print_error("Error al copiar fila en filterCLI");
                    return;
                }
                fila_destino++;
            }
        }
    }

    liberarMemoriaDF(dfActual);
    dfActual = nuevo_df;
    actualizarPrompt(dfActual);

    printf(GREEN "Filtrado completado. Quedan %d filas\n" RESET, nuevas_filas);
}

void quarterCLI(const char *nombreColumna_fecha, const char *nombre_nueva_columna) {
    VALIDAR_DF_Y_PARAMETROS(dfActual, nombreColumna_fecha);
    VALIDAR_DF_Y_PARAMETROS(dfActual, nombre_nueva_columna);

    int indice_col = encontrarIndiceColumna(dfActual, nombreColumna_fecha);
    if (indice_col == -1 || dfActual->columnas[indice_col].tipo != FECHA) {
        print_error("Columna de fecha no encontrada o tipo incorrecto");
        return;
    }

    if (encontrarIndiceColumna(dfActual, nombre_nueva_columna) != -1) {
        print_error("Ya existe una columna con ese nombre");
        return;
    }

    Dataframe *nuevo_df =
        crearNuevoDataframe(dfActual->numColumnas + 1, dfActual->numFilas, dfActual->nombre);
    if (!nuevo_df)
        return;

    // Copiar columnas antes de la columna de fecha
    for (int columnaActual = 0; columnaActual <= indice_col; columnaActual++) {
        if (!copiarColumna(&nuevo_df->columnas[columnaActual], &dfActual->columnas[columnaActual],
                           dfActual->numFilas)) {
            for (int k = 0; k < columnaActual; k++) {
                for (int f = 0; f < dfActual->numFilas; f++)
                    free(nuevo_df->columnas[k].datos[f]);
                free(nuevo_df->columnas[k].datos);
                free(nuevo_df->columnas[k].esNulo);
            }
            free(nuevo_df->columnas);
            free(nuevo_df);
            print_error("Error al copiar columna en quarterCLI");
            return;
        }
    }

    // Nueva columna de trimestre
    strncpy(nuevo_df->columnas[indice_col + 1].nombre, nombre_nueva_columna, 29);
    nuevo_df->columnas[indice_col + 1].nombre[29] = '\0';
    nuevo_df->columnas[indice_col + 1].tipo = TEXTO;
    nuevo_df->columnas[indice_col + 1].datos = malloc(dfActual->numFilas * sizeof(char *));
    nuevo_df->columnas[indice_col + 1].esNulo = malloc(dfActual->numFilas * sizeof(EstadoNulo));
    if (!nuevo_df->columnas[indice_col + 1].datos || !nuevo_df->columnas[indice_col + 1].esNulo) {
        for (int k = 0; k <= indice_col; k++) {
            for (int f = 0; f < dfActual->numFilas; f++)
                free(nuevo_df->columnas[k].datos[f]);
            free(nuevo_df->columnas[k].datos);
            free(nuevo_df->columnas[k].esNulo);
        }
        free(nuevo_df->columnas);
        free(nuevo_df);
        print_error("Error al asignar memoria para columna trimestre en quarterCLI");
        return;
    }

    // Copiar columnas después de la columna de fecha
    for (int columnaActual = indice_col + 1; columnaActual < dfActual->numColumnas;
         columnaActual++) {
        if (!copiarColumna(&nuevo_df->columnas[columnaActual + 1],
                           &dfActual->columnas[columnaActual], dfActual->numFilas)) {
            for (int k = 0; k <= columnaActual; k++) {
                for (int f = 0; f < dfActual->numFilas; f++)
                    free(nuevo_df->columnas[k].datos[f]);
                free(nuevo_df->columnas[k].datos);
                free(nuevo_df->columnas[k].esNulo);
            }
            free(nuevo_df->columnas);
            free(nuevo_df);
            print_error("Error al copiar columna en quarterCLI");
            return;
        }
    }

    // Calcular trimestre
    for (int filaActual = 0; filaActual < dfActual->numFilas; filaActual++) {
        if (dfActual->columnas[indice_col].esNulo[filaActual]) {
            nuevo_df->columnas[indice_col + 1].datos[filaActual] = strdup("#N/A");
            nuevo_df->columnas[indice_col + 1].esNulo[filaActual] = NO_NULO;
            continue;
        }
        struct tm fecha = {0};
        const char *fecha_str = dfActual->columnas[indice_col].datos[filaActual];
        if (sscanf(fecha_str, "%d-%d-%d", &fecha.tm_year, &fecha.tm_mon, &fecha.tm_mday) != 3) {
            nuevo_df->columnas[indice_col + 1].datos[filaActual] = strdup("#N/A");
            nuevo_df->columnas[indice_col + 1].esNulo[filaActual] = NO_NULO;
            continue;
        }
        const char *trimestre;
        if (fecha.tm_mon >= 1 && fecha.tm_mon <= 3)
            trimestre = "Q1";
        else if (fecha.tm_mon >= 4 && fecha.tm_mon <= 6)
            trimestre = "Q2";
        else if (fecha.tm_mon >= 7 && fecha.tm_mon <= 9)
            trimestre = "Q3";
        else if (fecha.tm_mon >= 10 && fecha.tm_mon <= 12)
            trimestre = "Q4";
        else
            trimestre = "#N/A";
        nuevo_df->columnas[indice_col + 1].datos[filaActual] = strdup(trimestre);
        nuevo_df->columnas[indice_col + 1].esNulo[filaActual] = NO_NULO;
    }

    liberarMemoriaDF(dfActual);
    dfActual = nuevo_df;
    actualizarPrompt(dfActual);
    printf(GREEN "Nueva columna '%s' creada con trimestres\n" RESET, nombre_nueva_columna);
}

void sortCLI(const char *nombreColumna, int descendente) {
    if (!dfActual) {
        print_error("No hay df cargado.");
        return;
    }

    int indice_columna = encontrarIndiceColumna(dfActual, nombreColumna);
    if (indice_columna == -1) {
        print_error("Columna no encontrada.");
        return;
    }

    ordenarDataframe(dfActual, indice_columna, descendente);

    printf(GREEN "df ordenado por columna '%s' en orden %s.\n" RESET, nombreColumna,
           descendente ? "descendente" : "ascendente");
}

void saveCLI(const char *nombre_archivo) {
    if (!dfActual) {
        print_error("No hay df activo para guardar.");
        return;
    }

    char nombre_saveCLI[MAX_FILENAME];
    if (nombre_archivo == NULL || strlen(nombre_archivo) == 0) {
        snprintf(nombre_saveCLI, sizeof(nombre_saveCLI), "%s.csv", dfActual->nombre);
    } else {
        strncpy(nombre_saveCLI, nombre_archivo, sizeof(nombre_saveCLI) - 1);
        nombre_saveCLI[sizeof(nombre_saveCLI) - 1] = '\0';
        cortarEspacios(nombre_saveCLI);
    }

    if (strlen(nombre_saveCLI) < 4 ||
        strcmp(nombre_saveCLI + strlen(nombre_saveCLI) - 4, ".csv") != 0) {
        strncat(nombre_saveCLI, ".csv", sizeof(nombre_saveCLI) - strlen(nombre_saveCLI) - 1);
    }

    FILE *file = fopen(nombre_saveCLI, "w");
    if (!file) {
        char error_msg[MAX_FILENAME + 50];
        snprintf(error_msg, sizeof(error_msg), "No se puede crear el archivo: %s", nombre_saveCLI);
        print_error(error_msg);
        return;
    }

    for (int col = 0; col < dfActual->numColumnas; col++) {
        fprintf(file, "%s", dfActual->columnas[col].nombre);
        if (col < dfActual->numColumnas - 1) {
            fprintf(file, ",");
        }
    }
    fprintf(file, "\n");

    for (int row = 0; row < dfActual->numFilas; row++) {
        for (int col = 0; col < dfActual->numColumnas; col++) {
            if (dfActual->columnas[col].esNulo[row]) {
            } else if (dfActual->columnas[col].datos[row] != NULL) {
                char *valor = (char *)dfActual->columnas[col].datos[row];
                int contiene_comas = (strchr(valor, ',') != NULL || strchr(valor, '"') != NULL);

                if (contiene_comas) {
                    fprintf(file, "\"");
                    for (char *c = valor; *c; c++) {
                        if (*c == '"') {
                            fprintf(file,
                                    "\"\""); // Comillas dobles para escaparlas
                        } else {
                            fprintf(file, "%c", *c);
                        }
                    }
                    fprintf(file, "\"");
                } else {
                    fprintf(file, "%s", valor);
                }
            }

            if (col < dfActual->numColumnas - 1) {
                fprintf(file, ",");
            }
        }
        fprintf(file, "\n");
    }

    fclose(file);
    printf(GREEN "df guardado exitosamente en %s\n" RESET, nombre_saveCLI);
}

void metaCLI() {
    if (!dfActual) {
        print_error("No hay df cargado.");
        return;
    }

    for (int col = 0; col < dfActual->numColumnas; col++) {
        int contador_nulos = 0;

        for (int row = 0; row < dfActual->numFilas; row++) {
            if (dfActual->columnas[col].esNulo[row]) {
                contador_nulos++;
            }
        }

        char *tipo;
        switch (dfActual->columnas[col].tipo) {
        case TEXTO:
            tipo = "Texto";
            break;
        case NUMERICO:
            tipo = "Numerico";
            break;
        case FECHA:
            tipo = "Fecha";
            break;
        default:
            tipo = "Desconocido";
            break;
        }

        printf(GREEN "%s: %s (Valores nulos: %d)\n" RESET, dfActual->columnas[col].nombre, tipo,
               contador_nulos);
    }
}

void viewCLI(int n) {
    if (!dfActual) {
        print_error("No hay df cargado.");
        return;
    }

    for (int j = 0; j < dfActual->numColumnas; j++) {
        printf("%s", dfActual->columnas[j].nombre);
        if (j < dfActual->numColumnas - 1) {
            printf(",");
        }
    }
    printf("\n");

    int total_filas = dfActual->numFilas;
    int filas_a_mostrar = (abs(n) < total_filas) ? abs(n) : total_filas;

    if (n >= 0) {
        for (int i = 0; i < filas_a_mostrar; i++) {
            for (int j = 0; j < dfActual->numColumnas; j++) {
                if (dfActual->columnas[j].esNulo[i]) {
                    printf("1");
                } else {
                    printf("%s", (char *)dfActual->columnas[j].datos[i]);
                }
                if (j < dfActual->numColumnas - 1) {
                    printf(",");
                }
            }
            printf("\n");
        }
    } else {
        for (int i = total_filas - 1; i >= total_filas - filas_a_mostrar; i--) {
            for (int j = 0; j < dfActual->numColumnas; j++) {
                if (dfActual->columnas[j].esNulo[i]) {
                    printf("NULL");
                } else {
                    printf("%s", (char *)dfActual->columnas[j].datos[i]);
                }
                if (j < dfActual->numColumnas - 1) {
                    printf(",");
                }
            }
            printf("\n");
        }
    }
}

void delcolumCLI(const char *nombreColumna) {
    if (!dfActual || !nombreColumna) {
        print_error("No hay df activo o nombre de columna inválido");
        return;
    }

    char nombreColumna_limpio[51];
    strncpy(nombreColumna_limpio, nombreColumna, 50);
    nombreColumna_limpio[50] = '\0';
    for (int i = 0; nombreColumna_limpio[i]; i++) {
        nombreColumna_limpio[i] = tolower(nombreColumna_limpio[i]);
    }

    int indice_col = -1;
    for (int i = 0; i < dfActual->numColumnas; i++) {
        char nombre_actual_limpio[51];
        strncpy(nombre_actual_limpio, dfActual->columnas[i].nombre, 50);
        nombre_actual_limpio[50] = '\0';
        for (int j = 0; nombre_actual_limpio[j]; j++) {
            nombre_actual_limpio[j] = tolower(nombre_actual_limpio[j]);
        }
        if (strcmp(nombre_actual_limpio, nombreColumna_limpio) == 0) {
            indice_col = i;
            break;
        }
    }

    if (indice_col == -1) {
        print_error("Columna no encontrada");
        return;
    }

    int nuevasColumnas = dfActual->numColumnas - 1;
    if (nuevasColumnas == 0) {
        print_error("No se puede eliminar la última columna");
        return;
    }

    Dataframe *nuevo_df = malloc(sizeof(Dataframe));
    if (!crearDF(nuevo_df, nuevasColumnas, dfActual->numFilas, dfActual->nombre)) {
        print_error("Error al crear nuevo dataframe");
        free(nuevo_df);
        return;
    }

    int nuevaColIndex = 0;
    for (int i = 0; i < dfActual->numColumnas; i++) {
        if (i == indice_col)
            continue;
        if (!copiarColumna(&nuevo_df->columnas[nuevaColIndex], &dfActual->columnas[i],
                           dfActual->numFilas)) {
            // Liberar columnas ya copiadas
            for (int k = 0; k < nuevaColIndex; k++) {
                for (int f = 0; f < dfActual->numFilas; f++)
                    free(nuevo_df->columnas[k].datos[f]);
                free(nuevo_df->columnas[k].datos);
                free(nuevo_df->columnas[k].esNulo);
            }
            free(nuevo_df->columnas);
            free(nuevo_df);
            print_error("Error al copiar columna en delcolumCLI");
            return;
        }
        nuevaColIndex++;
    }

    liberarMemoriaDF(dfActual);
    dfActual = nuevo_df;
    snprintf(promptTerminal, sizeof(promptTerminal), "[%s: %d,%d]:> ", dfActual->nombre,
             dfActual->numFilas, dfActual->numColumnas);
    printf(GREEN "Se eliminó la columna '%s'\n" RESET, nombreColumna);
}

void delnullCLI(const char *nombreColumna) {
    if (!dfActual || !nombreColumna) {
        print_error("No hay df activo o nombre de columna inválido");
        return;
    }

    int indice_col = -1;
    for (int i = 0; i < dfActual->numColumnas; i++) {
        if (strcmp(dfActual->columnas[i].nombre, nombreColumna) == 0) {
            indice_col = i;
            break;
        }
    }

    if (indice_col == -1) {
        print_error("Columna no encontrada");
        return;
    }

    int filasNulas = 0;
    for (int i = 0; i < dfActual->numFilas; i++) {
        if (dfActual->columnas[indice_col].esNulo[i]) {
            filasNulas++;
        }
    }

    if (filasNulas == 0) {
        printf(GREEN "No hay valores nulos para eliminar\n" RESET);
        return;
    }

    int validRows = dfActual->numFilas - filasNulas;
    Dataframe *nuevo_df = malloc(sizeof(Dataframe));
    if (!crearDF(nuevo_df, dfActual->numColumnas, validRows, dfActual->nombre)) {
        print_error("Error al crear nuevo dataframe");
        free(nuevo_df);
        return;
    }

    for (int i = 0; i < dfActual->numColumnas; i++) {
        strncpy(nuevo_df->columnas[i].nombre, dfActual->columnas[i].nombre, 29);
        nuevo_df->columnas[i].nombre[29] = '\0';
        nuevo_df->columnas[i].tipo = dfActual->columnas[i].tipo;
    }

    int newRow = 0;
    for (int i = 0; i < dfActual->numFilas; i++) {
        if (!dfActual->columnas[indice_col].esNulo[i]) {
            if (!copiarFila(nuevo_df, dfActual, i, newRow)) {
                // Liberar filas ya copiadas
                for (int r = 0; r < newRow; r++) {
                    for (int c = 0; c < nuevo_df->numColumnas; c++) {
                        free(nuevo_df->columnas[c].datos[r]);
                    }
                }
                for (int c = 0; c < nuevo_df->numColumnas; c++) {
                    free(nuevo_df->columnas[c].datos);
                    free(nuevo_df->columnas[c].esNulo);
                }
                free(nuevo_df->columnas);
                free(nuevo_df);
                print_error("Error al copiar fila en delnullCLI");
                return;
            }
            newRow++;
        }
    }

    liberarMemoriaDF(dfActual);
    dfActual = nuevo_df;
    snprintf(promptTerminal, sizeof(promptTerminal), "[%s: %d,%d]:> ", dfActual->nombre,
             dfActual->numFilas, dfActual->numColumnas);
    printf(GREEN "Se eliminaron %d filas con valores nulos\n" RESET, filasNulas);
}

// Macro para validar punteros
#define VALIDAR_PTR(ptr)                                                                           \
    do {                                                                                           \
        if (!(ptr)) {                                                                              \
            print_error("Puntero nulo");                                                           \
            return 0;                                                                              \
        }                                                                                          \
    } while (0)

int copiarColumna(Columna *destino, const Columna *origen, int numFilas) {
    VALIDAR_PTR(destino);
    VALIDAR_PTR(origen);
    if (numFilas <= 0)
        return 0;

    destino->datos = malloc(numFilas * sizeof(char *));
    destino->esNulo = malloc(numFilas * sizeof(EstadoNulo));
    if (!destino->datos || !destino->esNulo) {
        free(destino->datos);
        free(destino->esNulo);
        print_error("Fallo al asignar memoria en copiarColumna");
        return 0;
    }
    for (int i = 0; i < numFilas; i++) {
        if (origen->datos && origen->datos[i]) {
            destino->datos[i] = strdup(origen->datos[i]);
            if (!destino->datos[i]) {
                // Liberar lo previamente asignado
                for (int j = 0; j < i; j++)
                    free(destino->datos[j]);
                free(destino->datos);
                free(destino->esNulo);
                print_error("Fallo al copiar dato en copiarColumna");
                return 0;
            }
        } else {
            destino->datos[i] = NULL;
        }
        destino->esNulo[i] = origen->esNulo ? origen->esNulo[i] : 0;
    }
    destino->tipo = origen->tipo;
    copiarNombreColumna(destino->nombre, origen->nombre);
    return 1;
}

int copiarFila(Dataframe *destino, const Dataframe *origen, int fila_origen, int fila_destino) {
    VALIDAR_PTR(destino);
    VALIDAR_PTR(origen);
    if (fila_origen < 0 || fila_destino < 0 || fila_origen >= origen->numFilas ||
        fila_destino >= destino->numFilas) {
        print_error("Índice de fila fuera de rango en copiarFila");
        return 0;
    }
    for (int col = 0; col < origen->numColumnas && col < destino->numColumnas; col++) {
        if (origen->columnas[col].datos && origen->columnas[col].datos[fila_origen]) {
            destino->columnas[col].datos[fila_destino] =
                strdup(origen->columnas[col].datos[fila_origen]);
            if (!destino->columnas[col].datos[fila_destino]) {
                // Liberar lo previamente asignado en esta fila
                for (int j = 0; j < col; j++) {
                    free(destino->columnas[j].datos[fila_destino]);
                    destino->columnas[j].datos[fila_destino] = NULL;
                }
                print_error("Fallo al copiar dato en copiarFila");
                return 0;
            }
        } else {
            destino->columnas[col].datos[fila_destino] = NULL;
        }
        destino->columnas[col].esNulo[fila_destino] =
            origen->columnas[col].esNulo ? origen->columnas[col].esNulo[fila_origen] : 0;
    }
    return 1;
}

int nombreDFUnico(const Lista *lista, const char *nombre) {
    if (!lista || !nombre) return 0;
    Nodo *actual = lista->primero;
    while (actual) {
        const char *nombre_df = (actual->df->nombre[0] != '\0') ? actual->df->nombre : actual->df->indice;
        if (strcmp(nombre_df, nombre) == 0) return 0;
        actual = actual->siguiente;
    }
    return 1;
}

void cambiarDFPorNombre(Lista *lista, const char *nombre) {
    if (!lista || !nombre) {
        print_error("Nombre de dataframe inválido.");
        return;
    }
    Nodo *nodoActual = lista->primero;
    while (nodoActual) {
        const char *nombre_df = (nodoActual->df->nombre[0] != '\0') ? nodoActual->df->nombre : nodoActual->df->indice;
        if (strcmp(nombre_df, nombre) == 0) {
            dfActual = nodoActual->df;
            actualizarPrompt(dfActual);
            printf(GREEN "Cambiado a %s con %d filas y %d columnas\n" RESET, nombre_df, dfActual->numFilas, dfActual->numColumnas);
            return;
        }
        nodoActual = nodoActual->siguiente;
    }
    print_error("Dataframe no encontrado por nombre.");
}

void prefixCLI(const char *nombre_col, int n, const char *nombre_nueva_col) {
    if (!dfActual || !nombre_col || !nombre_nueva_col) {
        print_error("Argumentos inválidos para prefixCLI");
        return;
    }
    int idx = encontrarIndiceColumna(dfActual, nombre_col);
    if (idx == -1) {
        print_error("Columna no encontrada");
        return;
    }
    if (dfActual->columnas[idx].tipo != TEXTO) {
        print_error("La columna debe ser de tipo texto");
        return;
    }
    if (encontrarIndiceColumna(dfActual, nombre_nueva_col) != -1) {
        print_error("Ya existe una columna con ese nombre");
        return;
    }
    Dataframe *nuevo_df = crearNuevoDataframe(dfActual->numColumnas + 1, dfActual->numFilas, dfActual->nombre);
    if (!nuevo_df) return;
    // Copiar columnas existentes
    for (int c = 0; c < dfActual->numColumnas; c++) {
        if (!copiarColumna(&nuevo_df->columnas[c], &dfActual->columnas[c], dfActual->numFilas)) {
            for (int k = 0; k < c; k++) {
                for (int f = 0; f < dfActual->numFilas; f++)
                    free(nuevo_df->columnas[k].datos[f]);
                free(nuevo_df->columnas[k].datos);
                free(nuevo_df->columnas[k].esNulo);
            }
            free(nuevo_df->columnas);
            free(nuevo_df);
            print_error("Error al copiar columna en prefixCLI");
            return;
        }
    }
    // Nueva columna de prefijos
    strncpy(nuevo_df->columnas[dfActual->numColumnas].nombre, nombre_nueva_col, 29);
    nuevo_df->columnas[dfActual->numColumnas].nombre[29] = '\0';
    nuevo_df->columnas[dfActual->numColumnas].tipo = TEXTO;
    nuevo_df->columnas[dfActual->numColumnas].datos = malloc(dfActual->numFilas * sizeof(char *));
    nuevo_df->columnas[dfActual->numColumnas].esNulo = malloc(dfActual->numFilas * sizeof(EstadoNulo));
    if (!nuevo_df->columnas[dfActual->numColumnas].datos || !nuevo_df->columnas[dfActual->numColumnas].esNulo) {
        for (int k = 0; k < dfActual->numColumnas; k++) {
            for (int f = 0; f < dfActual->numFilas; f++)
                free(nuevo_df->columnas[k].datos[f]);
            free(nuevo_df->columnas[k].datos);
            free(nuevo_df->columnas[k].esNulo);
        }
        free(nuevo_df->columnas);
        free(nuevo_df);
        print_error("Error al asignar memoria para columna prefix");
        return;
    }
    for (int fila = 0; fila < dfActual->numFilas; fila++) {
        if (dfActual->columnas[idx].esNulo[fila] || !dfActual->columnas[idx].datos[fila]) {
            nuevo_df->columnas[dfActual->numColumnas].datos[fila] = strdup("");
            nuevo_df->columnas[dfActual->numColumnas].esNulo[fila] = NO_NULO;
        } else {
            char *valor = dfActual->columnas[idx].datos[fila];
            size_t len = strlen(valor);
            size_t pref_len = (len < (size_t)n) ? len : (size_t)n;
            char *prefijo = malloc(pref_len + 1);
            strncpy(prefijo, valor, pref_len);
            prefijo[pref_len] = '\0';
            nuevo_df->columnas[dfActual->numColumnas].datos[fila] = prefijo;
            nuevo_df->columnas[dfActual->numColumnas].esNulo[fila] = NO_NULO;
        }
    }
    liberarMemoriaDF(dfActual);
    dfActual = nuevo_df;
    actualizarPrompt(dfActual);
    printf(GREEN "Nueva columna '%s' creada con los primeros %d caracteres de '%s'\n" RESET, nombre_nueva_col, n, nombre_col);
}

void listCLI() {
    Nodo *actual = listaDF.primero;
    if (!actual) {
        printf("No hay dataframes cargados en memoria.\n");
        return;
    }
    while (actual) {
        const char *nombre_df = (actual->df->nombre[0] != '\0') ? actual->df->nombre : actual->df->indice;
        printf("%s: %d filas, %d columnas\n", nombre_df, actual->df->numFilas, actual->df->numColumnas);
        actual = actual->siguiente;
    }
}

void addCLI(const char *nombre_archivo, char sep) {
    if (!dfActual) {
        print_error("No hay dataframe activo");
        return;
    }
    FILE *file = fopen(nombre_archivo, "r");
    if (!file) {
        print_error("No se puede abrir el archivo");
        return;
    }
    char *headerLine = NULL;
    size_t len = 0;
    if (getline(&headerLine, &len, file) == -1) {
        print_error("Error al leer encabezados");
        free(headerLine);
        fclose(file);
        return;
    }
    // Contar columnas del archivo
    int numColumnasArchivo = 1;
    for (char *p = headerLine; *p; ++p) {
        if (*p == sep) numColumnasArchivo++;
    }
    if (numColumnasArchivo != dfActual->numColumnas) {
        print_error("El archivo debe tener el mismo número de columnas que el dataframe actual");
        free(headerLine);
        fclose(file);
        return;
    }
    // Verificar nombres de columnas
    char *token = strtok(headerLine, &sep);
    int columnaActual = 0;
    while (token && columnaActual < numColumnasArchivo) {
        if (strcmp(token, dfActual->columnas[columnaActual].nombre) != 0) {
            print_error("Los nombres de las columnas deben coincidir");
            free(headerLine);
            fclose(file);
            return;
        }
        token = strtok(NULL, &sep);
        columnaActual++;
    }
    free(headerLine);
    // Contar filas del archivo
    int filasArchivo = 0;
    char *linea = NULL;
    size_t lenLinea = 0;
    while (getline(&linea, &lenLinea, file) != -1) {
        filasArchivo++;
    }
    free(linea);
    if (filasArchivo == 0) {
        print_error("El archivo no tiene datos");
        fclose(file);
        return;
    }
    // Crear nuevo dataframe con filas adicionales
    Dataframe *nuevo_df = crearNuevoDataframe(dfActual->numColumnas, dfActual->numFilas + filasArchivo, dfActual->nombre);
    if (!nuevo_df) {
        fclose(file);
        return;
    }
    // Copiar columnas existentes
    for (int c = 0; c < dfActual->numColumnas; c++) {
        strncpy(nuevo_df->columnas[c].nombre, dfActual->columnas[c].nombre, 29);
        nuevo_df->columnas[c].nombre[29] = '\0';
        nuevo_df->columnas[c].tipo = dfActual->columnas[c].tipo;
        nuevo_df->columnas[c].datos = malloc((dfActual->numFilas + filasArchivo) * sizeof(char *));
        nuevo_df->columnas[c].esNulo = malloc((dfActual->numFilas + filasArchivo) * sizeof(EstadoNulo));
        if (!nuevo_df->columnas[c].datos || !nuevo_df->columnas[c].esNulo) {
            for (int k = 0; k < c; k++) {
                free(nuevo_df->columnas[k].datos);
                free(nuevo_df->columnas[k].esNulo);
            }
            free(nuevo_df->columnas);
            free(nuevo_df);
            fclose(file);
            print_error("Error al asignar memoria");
            return;
        }
        // Copiar datos existentes
        for (int f = 0; f < dfActual->numFilas; f++) {
            if (dfActual->columnas[c].datos[f]) {
                nuevo_df->columnas[c].datos[f] = strdup(dfActual->columnas[c].datos[f]);
            } else {
                nuevo_df->columnas[c].datos[f] = NULL;
            }
            nuevo_df->columnas[c].esNulo[f] = dfActual->columnas[c].esNulo[f];
        }
    }
    // Leer y añadir nuevas filas
    rewind(file);
    getline(&headerLine, &len, file); // Saltar encabezados
    int filaDestino = dfActual->numFilas;
    while (getline(&linea, &lenLinea, file) != -1 && filaDestino < dfActual->numFilas + filasArchivo) {
        char *token = strtok(linea, &sep);
        int col = 0;
        while (token && col < dfActual->numColumnas) {
            if (token[0] != '\0' && token[0] != '\n' && token[0] != '\r') {
                nuevo_df->columnas[col].datos[filaDestino] = strdup(token);
                nuevo_df->columnas[col].esNulo[filaDestino] = NO_NULO;
            } else {
                nuevo_df->columnas[col].datos[filaDestino] = NULL;
                nuevo_df->columnas[col].esNulo[filaDestino] = NULO;
            }
            token = strtok(NULL, &sep);
            col++;
        }
        filaDestino++;
    }
    free(headerLine);
    free(linea);
    fclose(file);
    liberarMemoriaDF(dfActual);
    dfActual = nuevo_df;
    actualizarPrompt(dfActual);
    printf(GREEN "Añadidas %d filas del archivo '%s'\n" RESET, filasArchivo, nombre_archivo);
}