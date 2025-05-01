#include "lib.h"

// Declaraciones de funciones estáticas
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

// Variables globales
Lista listaDF = {0, NULL};
Dataframe *dfActual = NULL;
char promptTerminal[MAX_LINE_LENGTH] = "[?]:> ";

// Implementación de funciones helper
Dataframe* crearNuevoDataframe(int numColumnas, int numFilas, const char* indice) {
    Dataframe* df = malloc(sizeof(Dataframe));
    if (!df || !crearDF(df, numColumnas, numFilas, indice)) {
        free(df);
        print_error("Error al crear nuevo dataframe");
        return NULL;
    }
    return df;
}

void copiarNombreColumna(char* destino, const char* origen) {
    strncpy(destino, origen, MAX_NOMBRE_COLUMNA - 1);
    destino[MAX_NOMBRE_COLUMNA - 1] = '\0';
}

char* copiarDatoSeguro(const char* origen) {
    return origen ? strdup(origen) : NULL;
}

void actualizarPrompt(const Dataframe* df) {
    if (!df) {
        strncpy(promptTerminal, "[?]:> ", MAX_LINE_LENGTH);
        return;
    }
    snprintf(promptTerminal, MAX_LINE_LENGTH, "[%s: %d,%d]:> ",
             df->indice, df->numFilas, df->numColumnas);
}

void liberarRecursosEnError(Dataframe* df, const char* mensaje) {
    if (df) liberarMemoriaDF(df);
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
                char **nuevos_datos = realloc(df->columnas[columnaActual].datos,
                                            numMaxFilas * sizeof(char *));
                EstadoNulo *nuevos_nulos = realloc(df->columnas[columnaActual].esNulo,
                                                  numMaxFilas * sizeof(EstadoNulo));

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
    if (!dato1 || !dato2) return 0;

    switch (tipo) {
        case NUMERICO: {
            char *str1 = (char *)dato1;
            char *str2 = (char *)dato2;
            double val1, val2;
            char *endptr1, *endptr2;

            val1 = strtod(str1, &endptr1);
            val2 = strtod(str2, &endptr2);

            // Verificar si la conversión fue exitosa
            if (endptr1 == str1 || endptr2 == str2) {
                return 0;
            }

            if (strcmp(operador, "eq") == 0) return fabs(val1 - val2) < 1e-10;
            if (strcmp(operador, "neq") == 0) return fabs(val1 - val2) >= 1e-10;
            if (strcmp(operador, "gt") == 0) return val1 > val2;
            if (strcmp(operador, "lt") == 0) return val1 < val2;
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

            if (strcmp(operador, "eq") == 0) return time1 == time2;
            if (strcmp(operador, "neq") == 0) return time1 != time2;
            if (strcmp(operador, "gt") == 0) return time1 > time2;
            if (strcmp(operador, "lt") == 0) return time1 < time2;
            break;
        }
        case TEXTO: {
            int comp = strcmp((char *)dato1, (char *)dato2);
            if (strcmp(operador, "eq") == 0) return comp == 0;
            if (strcmp(operador, "neq") == 0) return comp != 0;
            if (strcmp(operador, "gt") == 0) return comp > 0;
            if (strcmp(operador, "lt") == 0) return comp < 0;
            break;
        }
    }
    return 0;
}

void filterCLI(Dataframe *df, const char *nombreColumnaumna, const char *operador, void *valor) {
    VALIDAR_DF_Y_PARAMETROS(df, nombreColumnaumna);
    VALIDAR_DF_Y_PARAMETROS(df, operador);
    VALIDAR_DF_Y_PARAMETROS(df, valor);

    int indice_col = encontrarIndiceColumna(df, nombreColumnaumna);
    if (indice_col == -1) {
        print_error("Columna no encontrada");
        return;
    }

    if (strcmp(operador, "eq") != 0 && strcmp(operador, "neq") != 0 &&
        strcmp(operador, "gt") != 0 && strcmp(operador, "lt") != 0) {
        print_error("Operador inválido. Use: eq, neq, gt, lt");
        return;
    }

    // Primero contar cuántas filas cumplen la condición
    int nuevas_filas = 0;
    for (int filaActual = 0; filaActual < df->numFilas; filaActual++) {
        if (!df->columnas[indice_col].esNulo[filaActual]) {
            void *valor_actual = df->columnas[indice_col].datos[filaActual];
            if (valor_actual && comparar(valor_actual, valor,
                        df->columnas[indice_col].tipo, operador)) {
                nuevas_filas++;
            }
        }
    }

    if (nuevas_filas == 0) {
        printf(GREEN "No se encontraron filas que cumplan la condición\n" RESET);
        return;
    }

    // Crear nuevo dataframe con el tamaño exacto
    Dataframe *nuevo_df = malloc(sizeof(Dataframe));
    if (!nuevo_df) {
        print_error("Error al asignar memoria para el nuevo dataframe");
        return;
    }

    if (!crearDF(nuevo_df, df->numColumnas, nuevas_filas, df->indice)) {
        free(nuevo_df);
        print_error("Error al crear el nuevo dataframe");
        return;
    }

    // Copiar nombres y tipos de columnas
    for (int columnaActual = 0; columnaActual < df->numColumnas; columnaActual++) {
        copiarNombreColumna(nuevo_df->columnas[columnaActual].nombre,
                           df->columnas[columnaActual].nombre);
        nuevo_df->columnas[columnaActual].tipo = df->columnas[columnaActual].tipo;
    }

    // Copiar solo las filas que cumplen la condición
    int fila_destino = 0;
    for (int filaActual = 0; filaActual < df->numFilas; filaActual++) {
        if (!df->columnas[indice_col].esNulo[filaActual]) {
            void *valor_actual = df->columnas[indice_col].datos[filaActual];
            if (valor_actual && comparar(valor_actual, valor,
                        df->columnas[indice_col].tipo, operador)) {
                for (int columnaActual = 0; columnaActual < df->numColumnas; columnaActual++) {
                    nuevo_df->columnas[columnaActual].datos[fila_destino] =
                        copiarDatoSeguro(df->columnas[columnaActual].datos[filaActual]);
                    nuevo_df->columnas[columnaActual].esNulo[fila_destino] =
                        df->columnas[columnaActual].esNulo[filaActual];
                }
                fila_destino++;
            }
        }
    }

    // Liberar el dataframe anterior y actualizar el actual
    liberarMemoriaDF(dfActual);
    dfActual = nuevo_df;
    actualizarPrompt(dfActual);

    printf(GREEN "Filtrado completado. Quedan %d filas\n" RESET, nuevas_filas);
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

void quarterCLI(const char *nombreColumnaumna_fecha, const char *nombre_nueva_columna) {
    VALIDAR_DF_Y_PARAMETROS(dfActual, nombreColumnaumna_fecha);
    VALIDAR_DF_Y_PARAMETROS(dfActual, nombre_nueva_columna);

    int indice_col = encontrarIndiceColumna(dfActual, nombreColumnaumna_fecha);
    if (indice_col == -1 || dfActual->columnas[indice_col].tipo != FECHA) {
        print_error("Columna de fecha no encontrada o tipo incorrecto");
        return;
    }

    // Verificar que el nombre de la nueva columna no existe
    if (encontrarIndiceColumna(dfActual, nombre_nueva_columna) != -1) {
        print_error("Ya existe una columna con ese nombre");
        return;
    }

    Dataframe *nuevo_df = crearNuevoDataframe(dfActual->numColumnas + 1,
                                            dfActual->numFilas,
                                            dfActual->indice);
    if (!nuevo_df) return;

    // Copiar columnas existentes hasta la columna de fecha
    for (int columnaActual = 0; columnaActual <= indice_col; columnaActual++) {
        copiarNombreColumna(nuevo_df->columnas[columnaActual].nombre,
                           dfActual->columnas[columnaActual].nombre);
        nuevo_df->columnas[columnaActual].tipo = dfActual->columnas[columnaActual].tipo;

        for (int filaActual = 0; filaActual < dfActual->numFilas; filaActual++) {
            nuevo_df->columnas[columnaActual].datos[filaActual] =
                copiarDatoSeguro(dfActual->columnas[columnaActual].datos[filaActual]);
            nuevo_df->columnas[columnaActual].esNulo[filaActual] =
                dfActual->columnas[columnaActual].esNulo[filaActual];
        }
    }

    // Configurar la nueva columna
    copiarNombreColumna(nuevo_df->columnas[indice_col + 1].nombre, nombre_nueva_columna);
    nuevo_df->columnas[indice_col + 1].tipo = TEXTO;

    // Copiar el resto de las columnas después de la nueva
    for (int columnaActual = indice_col + 1; columnaActual < dfActual->numColumnas; columnaActual++) {
        copiarNombreColumna(nuevo_df->columnas[columnaActual + 1].nombre,
                           dfActual->columnas[columnaActual].nombre);
        nuevo_df->columnas[columnaActual + 1].tipo = dfActual->columnas[columnaActual].tipo;

        for (int filaActual = 0; filaActual < dfActual->numFilas; filaActual++) {
            nuevo_df->columnas[columnaActual + 1].datos[filaActual] =
                copiarDatoSeguro(dfActual->columnas[columnaActual].datos[filaActual]);
            nuevo_df->columnas[columnaActual + 1].esNulo[filaActual] =
                dfActual->columnas[columnaActual].esNulo[filaActual];
        }
    }

    // Calcular trimestres para la nueva columna
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
        if (fecha.tm_mon >= 1 && fecha.tm_mon <= 3) trimestre = "Q1";
        else if (fecha.tm_mon >= 4 && fecha.tm_mon <= 6) trimestre = "Q2";
        else if (fecha.tm_mon >= 7 && fecha.tm_mon <= 9) trimestre = "Q3";
        else if (fecha.tm_mon >= 10 && fecha.tm_mon <= 12) trimestre = "Q4";
        else trimestre = "#N/A";

        nuevo_df->columnas[indice_col + 1].datos[filaActual] = strdup(trimestre);
        nuevo_df->columnas[indice_col + 1].esNulo[filaActual] = NO_NULO;
    }

    liberarMemoriaDF(dfActual);
    dfActual = nuevo_df;
    actualizarPrompt(dfActual);

    printf(GREEN "Nueva columna '%s' creada con trimestres\n" RESET, nombre_nueva_columna);
}

void CLI() {
  char input[MAX_LINE_LENGTH];

  while (1) {
    printf("%s", promptTerminal);
    fgets(input, sizeof(input), stdin);
    cortarEspacios(input);

    if (strcmp(input, "quit") == 0) {
      printf(GREEN "EXIT PROGRAM\n" RESET);
      break;
    }
    else if (strncmp(input, "load ", 5) == 0) {
      loadearCSV(input + 5);
    }
    else if (strcmp(input, "meta") == 0) {
      metaCLI();
    }
    else if (strncmp(input, "save", 4) == 0) {
      saveCLI(input + 4);
    }
    else if (strncmp(input, "view", 4) == 0) {
      int n = 10;
      if (strlen(input) > 4) {
        char *endptr;
        long parsed_n = strtol(input + 4, &endptr, 10);
        if (endptr != input + 4 && parsed_n > 0) {
          n = (int)parsed_n;
        } else {
          print_error("Número de filas no válido");
          continue;
        }
      }
      viewCLI(n);
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
    }
    else if (strncmp(input, "delnull ", 8) == 0) {
      delnullCLI(input + 8);
    }
    else if (strncmp(input, "delcolum ", 9) == 0) {
      const char *nombreColumnaumna = input + 9;
      while (*nombreColumnaumna == ' ') nombreColumnaumna++;
      delcolumCLI(nombreColumnaumna);
    }
    else if (strncmp(input, "df", 2) == 0 && strlen(input) > 2) {
      char *endptr;
      long indice = strtol(input + 2, &endptr, 10);

      if (*endptr == '\0' && indice >= 0 && indice < listaDF.numDFs) {
        cambiarDF(&listaDF, indice);
      } else {
        print_error("Índice de dataframe inválido.");
      }
    }
    else if (strncmp(input, "filter ", 7) == 0) {
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

      // Crear una copia del valor para evitar problemas de memoria
      char *valor_convertido = strdup(valor);
      if (!valor_convertido) {
        print_error("Error de memoria");
        continue;
      }

      filterCLI(dfActual, columna, operador, valor_convertido);
      free(valor_convertido);
    }
    else if (strncmp(input, "quarter ", 8) == 0) {
      char col_fecha[50], col_nueva[50];

      int parsed = sscanf(input + 8, "%49s %49s", col_fecha, col_nueva);
      if (parsed != 2) {
        print_error("Uso: quarter [columna_fecha] [nombre_nueva_columna]");
        continue;
      }

      quarterCLI(col_fecha, col_nueva);
    }
  }
}

void agregarDF(Dataframe *nuevoDF) {
  if (!nuevoDF) {
    print_error("Dataframe inválido");
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
  if (!crearDF(nuevo_df, nuevasColumnas, dfActual->numFilas, dfActual->indice)) {
    print_error("Error al crear nuevo dataframe");
    free(nuevo_df);
    return;
  }

  int nuevaColIndex = 0;
  for (int i = 0; i < dfActual->numColumnas; i++) {
    if (i == indice_col) continue;

    strncpy(nuevo_df->columnas[nuevaColIndex].nombre, dfActual->columnas[i].nombre, 50);
    nuevo_df->columnas[nuevaColIndex].tipo = dfActual->columnas[i].tipo;

    for (int j = 0; j < dfActual->numFilas; j++) {
      nuevo_df->columnas[nuevaColIndex].datos[j] =
          dfActual->columnas[i].datos[j] ? strdup(dfActual->columnas[i].datos[j]) : NULL;
      nuevo_df->columnas[nuevaColIndex].esNulo[j] = dfActual->columnas[i].esNulo[j];
    }

    nuevaColIndex++;
  }

  liberarMemoriaDF(dfActual);
  dfActual = nuevo_df;

  snprintf(promptTerminal, sizeof(promptTerminal), "[%s: %d,%d]:> ",
           dfActual->indice, dfActual->numFilas, dfActual->numColumnas);

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
  if (!crearDF(nuevo_df, dfActual->numColumnas, validRows, dfActual->indice)) {
    print_error("Error al crear nuevo dataframe");
    free(nuevo_df);
    return;
  }

  for (int i = 0; i < dfActual->numColumnas; i++) {
    strncpy(nuevo_df->columnas[i].nombre, dfActual->columnas[i].nombre, 50);
    nuevo_df->columnas[i].tipo = dfActual->columnas[i].tipo;
  }

  int newRow = 0;
  for (int i = 0; i < dfActual->numFilas; i++) {
    if (!dfActual->columnas[indice_col].esNulo[i]) {
      for (int j = 0; j < dfActual->numColumnas; j++) {
        nuevo_df->columnas[j].datos[newRow] =
            dfActual->columnas[j].datos[i] ? strdup(dfActual->columnas[j].datos[i]) : NULL;
        nuevo_df->columnas[j].esNulo[newRow] = dfActual->columnas[j].esNulo[i];
      }
      newRow++;
    }
  }

  liberarMemoriaDF(dfActual);
  dfActual = nuevo_df;

  snprintf(promptTerminal, sizeof(promptTerminal), "[%s: %d,%d]:> ",
           dfActual->indice, dfActual->numFilas, dfActual->numColumnas);

  printf(GREEN "Se eliminaron %d filas con valores nulos\n" RESET, filasNulas);
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
  strncpy(df->indice, nombre_df, sizeof(df->indice) - 1);
  df->indice[sizeof(df->indice) - 1] = '\0';

  return 1;
}

void loadearCSV(const char *nombre_archivo) {
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

    int numColumnas = contarColumnas(headerLine);
    if (numColumnas > MAX_COLUMNS) {
        print_error("Demasiadas columnas");
        free(headerLine);
        fclose(file);
        return;
    }

    char nombre_df[MAX_INDICE_LENGTH];
    snprintf(nombre_df, sizeof(nombre_df), "df%d", listaDF.numDFs);

    Dataframe *nuevo_df = crearNuevoDataframe(numColumnas, BATCH_SIZE, nombre_df);
    if (!nuevo_df) {
        free(headerLine);
        fclose(file);
        return;
    }

    char *token = strtok(headerLine, ",\n\r");
    int columnaActual = 0;
    while (token && columnaActual < numColumnas) {
        copiarNombreColumna(nuevo_df->columnas[columnaActual].nombre, token);
        columnaActual++;
        token = strtok(NULL, ",\n\r");
    }

    free(headerLine);

    procesarPorLotes(file, nuevo_df, BATCH_SIZE);
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

    printf(GREEN "Archivo cargado: %d filas, %d columnas\n" RESET,
           dfActual->numFilas, dfActual->numColumnas);
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
    if (line[i] == ',') count++;
  }
  return count;
}

int fechaValida(const char *str_fecha) {
    if (!str_fecha) return 0;
    
    int año, mes, dia;
    if (sscanf(str_fecha, "%4d-%2d-%2d", &año, &mes, &dia) != 3) return 0;

    // Validar rangos básicos
    if (año < 1900 || mes < 1 || mes > 12 || dia < 1 || dia > 31) return 0;

    // Días por mes, incluyendo bisiestos
    int diasPorMes[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    if ((año % 4 == 0 && año % 100 != 0) || (año % 400 == 0)) diasPorMes[1] = 29;

    return (dia <= diasPorMes[mes - 1]);
}

void tiposColumnas(Dataframe *df) {
    if (!df) return;
    
    for (int col = 0; col < df->numColumnas; col++) {
        int esFecha = 1;
        for (int fila = 0; fila < df->numFilas; fila++) {
            if (df->columnas[col].esNulo[fila]) continue;
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
  if (a == NULL && b == NULL) return 0;
  if (a == NULL) return esta_desc ? 1 : -1;
  if (b == NULL) return esta_desc ? -1 : 1;

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
  if (indice < 0 || indice >= lista->numDFs) {
    print_error("Índice de dataframe inválido.");
    return;
  }

  int indiceInverso = lista->numDFs - 1 - indice;
  Nodo *nodoActual = lista->primero;

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
  snprintf(promptTerminal, sizeof(promptTerminal), "[%s: %d,%d]:> ",
           dfActual->indice, dfActual->numFilas, dfActual->numColumnas);
  printf(GREEN "Cambiado a %s con %d filas y %d columnas\n" RESET,
         nodoActual->df->indice, nodoActual->df->numFilas,
         nodoActual->df->numColumnas);
}
void saveCLI(const char *nombre_archivo) {
  if (!dfActual) {
    print_error("No hay df activo para guardar.");
    return;
  }

  char nombre_saveCLI[MAX_FILENAME];
  if (nombre_archivo == NULL || strlen(nombre_archivo) == 0) {
    snprintf(nombre_saveCLI, sizeof(nombre_saveCLI), "%s.csv",
             dfActual->indice);
  } else {
    strncpy(nombre_saveCLI, nombre_archivo, sizeof(nombre_saveCLI) - 1);
    nombre_saveCLI[sizeof(nombre_saveCLI) - 1] = '\0';
    cortarEspacios(nombre_saveCLI);
  }

  if (strlen(nombre_saveCLI) < 4 ||
      strcmp(nombre_saveCLI + strlen(nombre_saveCLI) - 4, ".csv") != 0) {
    strncat(nombre_saveCLI, ".csv",
            sizeof(nombre_saveCLI) - strlen(nombre_saveCLI) - 1);
  }

  FILE *file = fopen(nombre_saveCLI, "w");
  if (!file) {
    char error_msg[MAX_FILENAME + 50];
    snprintf(error_msg, sizeof(error_msg), "No se puede crear el archivo: %s",
             nombre_saveCLI);
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
        int contiene_comas =
            (strchr(valor, ',') != NULL || strchr(valor, '"') != NULL);

        if (contiene_comas) {
          fprintf(file, "\"");
          for (char *c = valor; *c; c++) {
            if (*c == '"') {
              fprintf(file,
                      "\"\"");  // Comillas dobles para escaparlas
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

    printf(GREEN "%s: %s (Valores nulos: %d)\n" RESET,
           dfActual->columnas[col].nombre, tipo, contador_nulos);
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

  int filas_a_mostrar = (n < dfActual->numFilas) ? n : dfActual->numFilas;

  for (int i = 0; i < filas_a_mostrar; i++) {
    for (int j = 0; j < dfActual->numColumnas; j++) {
      if (dfActual->columnas[j].esNulo[i]) {
        printf("1");

      }
     else {
          printf("%s", (char *)dfActual->columnas[j].datos[i]);
      }

      if (j < dfActual->numColumnas - 1) {
        printf(",");
      }
    }

    printf("\n");
  }
}

int encontrarIndiceColumna(Dataframe *df, const char *nombreColumnaumna) {
  for (int i = 0; i < df->numColumnas; i++) {
    if (strcmp(df->columnas[i].nombre, nombreColumnaumna) == 0) {
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

void sortCLI(const char *nombreColumnaumna, int descendente) {
  if (!dfActual) {
    print_error("No hay df cargado.");
    return;
  }

  int indice_columna = encontrarIndiceColumna(dfActual, nombreColumnaumna);
  if (indice_columna == -1) {
    print_error("Columna no encontrada.");
    return;
  }

  ordenarDataframe(dfActual, indice_columna, descendente);

  printf(GREEN "df ordenado por columna '%s' en orden %s.\n" RESET,
         nombreColumnaumna, descendente ? "descendente" : "ascendente");
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

            if (i < longitud) resultado[j++] = lineaLeida[i];

            inicio_campo = i + 1;
            indice_columna++;
        }
    }
    resultado[j] = '\0';
}

