#define _POSIX_C_SOURCE 200809L
#include <sys/types.h>
#include "lib.h"

Lista listaDF = {0, NULL};    
Dataframe *df_actual = NULL;  
char promptTerminal[MAX_LINE_LENGTH] = "[?]:> ";

void procesarPorLotes(FILE *archivo, Dataframe *df, int tamanoLote) {
  char *lineaLeida = NULL;
  size_t len = 0;
  int filaActual = 0;
  int numMaxFilas = tamanoLote;

  while (getline(&lineaLeida, &len, archivo) != -1) {
    if (filaActual >= numMaxFilas) {
      numMaxFilas += tamanoLote;

      for (int i = 0; i < df->numColumnas; i++) {
        char **nuevos_datos = realloc(df->columnas[i].datos, numMaxFilas * sizeof(char *));
        unsigned int *nuevos_nulos = realloc(df->columnas[i].esNulo, numMaxFilas * sizeof(unsigned int *));

        if (!nuevos_datos || !nuevos_nulos) {
          print_error("Error al expandir memoria");
          free(lineaLeida);
          return;
        }

        df->columnas[i].datos = nuevos_datos;
        df->columnas[i].esNulo = nuevos_nulos;

        for (int j = filaActual; j < numMaxFilas; j++) {
          df->columnas[i].datos[j] = NULL;
          df->columnas[i].esNulo[j] = 0;
        }
      }
    }

    verificarNulos(lineaLeida, filaActual, df);
    char *token = strtok(lineaLeida, ",\n\r");
    int col = 0;

    while (token && col < df->numColumnas) {
      if (!df->columnas[col].esNulo[filaActual]) {
        df->columnas[col].datos[filaActual] = strdup(token);
      }
      token = strtok(NULL, ",\n\r");
      col++;
    }
    filaActual++;
  }

  df->numFilas = filaActual;

  for (int i = 0; i < df->numColumnas; i++) {
    df->columnas[i].datos = realloc(df->columnas[i].datos, filaActual * sizeof(char *));
    df->columnas[i].esNulo = realloc(df->columnas[i].esNulo, filaActual * sizeof(int));
  }

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
      int val1 = atoi((char *)dato1);
      int val2 = atoi((char *)dato2);

      if (strcmp(operador, "eq") == 0) return val1 == val2;
      if (strcmp(operador, "neq") == 0) return val1 != val2;
      if (strcmp(operador, "gt") == 0) return val1 > val2;
      if (strcmp(operador, "lt") == 0) return val1 < val2;
      break;
    }
    case FECHA: {
      struct tm tm1 = {0}, tm2 = {0};
      sscanf((char *)dato1, "%d-%d-%d", &tm1.tm_year, &tm1.tm_mon, &tm1.tm_mday);
      sscanf((char *)dato2, "%d-%d-%d", &tm2.tm_year, &tm2.tm_mon, &tm2.tm_mday);

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

void filterCLI(Dataframe *df, const char *nombre_columna, const char *operador, void *valor) {
  if (!df || !nombre_columna || !operador || !valor) {
    print_error("Parámetros inválidos para el filtrado");
    return;
  }

  int indice_col = -1;
  for (int i = 0; i < df->numColumnas; i++) {
    if (strcmp(df->columnas[i].nombre, nombre_columna) == 0) {
      indice_col = i;
      break;
    }
  }

  if (indice_col == -1) {
    print_error("Columna no encontrada");
    return;
  }

  if (strcmp(operador, "eq") != 0 && strcmp(operador, "neq") != 0 &&
      strcmp(operador, "gt") != 0 && strcmp(operador, "lt") != 0) {
    print_error("Operador inválido. Use: eq, neq, gt, lt");
    return;
  }

  Dataframe *nuevo_df = malloc(sizeof(Dataframe));
  if (!crearDF(nuevo_df, df->numColumnas, df->numFilas, df->indice)) {
    print_error("Error al crear nuevo dataframe");
    free(nuevo_df);
    return;
  }

  for (int i = 0; i < df->numColumnas; i++) {
    strncpy(nuevo_df->columnas[i].nombre, df->columnas[i].nombre, 50);
    nuevo_df->columnas[i].tipo = df->columnas[i].tipo;
  }

  int nuevas_filas = 0;
  for (int i = 0; i < df->numFilas; i++) {
    if (df->columnas[indice_col].esNulo[i]) {
      continue;
    }

    if (comparar(df->columnas[indice_col].datos[i], valor,
                 df->columnas[indice_col].tipo, operador)) {
      for (int j = 0; j < df->numColumnas; j++) {
        nuevo_df->columnas[j].datos[nuevas_filas] =
            df->columnas[j].datos[i] ? strdup(df->columnas[j].datos[i]) : NULL;
        nuevo_df->columnas[j].esNulo[nuevas_filas] = df->columnas[j].esNulo[i];
      }
      nuevas_filas++;
    }
  }

  nuevo_df->numFilas = nuevas_filas;
  liberarMemoriaDF(df_actual);
  df_actual = nuevo_df;

  snprintf(promptTerminal, sizeof(promptTerminal), "[%s: %d,%d]:> ",
           df_actual->indice, df_actual->numFilas, df_actual->numColumnas);

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
      char nombre_col[50];
      char order[10] = "asc";
      int esta_desc = 0;

      int parsed = sscanf(input + 5, "%49s %9s", nombre_col, order);
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
      sortCLI(nombre_col, esta_desc);
    }
    else if (strncmp(input, "delnull ", 8) == 0) {
      delnullCLI(input + 8);
    } else if (strncmp(input, "delcolum ", 9) == 0) {
      const char *nombre_columna = input + 9;
      while (*nombre_columna == ' ') nombre_columna++;
      delcolumCLI(nombre_columna);
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

      if (!df_actual) {
        print_error("No hay dataframe activo");
        continue;
      }

      int col_idx = -1;
      for (int i = 0; i < df_actual->numColumnas; i++) {
        if (strcmp(df_actual->columnas[i].nombre, columna) == 0) {
          col_idx = i;
          break;
        }
      }

      if (col_idx == -1) {
        print_error("Columna no encontrada");
        continue;
      }

      void *valor_convertido = NULL;
      switch (df_actual->columnas[col_idx].tipo) {
        case NUMERICO: {
          double *num_val = malloc(sizeof(double));
          if (!num_val) {
            print_error("Error de memoria");
            continue;
          }
          *num_val = atof(valor);
          valor_convertido = num_val;
          break;
        }
        case FECHA:
        case TEXTO:
          valor_convertido = strdup(valor);
          if (!valor_convertido) {
            print_error("Error de memoria");
            continue;
          }
          break;
      }

      filterCLI(df_actual, columna, operador, valor_convertido);
      free(valor_convertido);
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

void delcolumCLI(const char *nombre_col) {
  if (!df_actual || !nombre_col) {
    print_error("No hay df activo o nombre de columna inválido");
    return;
  }

  char nombre_col_limpio[51];
  strncpy(nombre_col_limpio, nombre_col, 50);
  nombre_col_limpio[50] = '\0';
  for (int i = 0; nombre_col_limpio[i]; i++) {
    nombre_col_limpio[i] = tolower(nombre_col_limpio[i]);
  }

  int indice_col = -1;
  for (int i = 0; i < df_actual->numColumnas; i++) {
    char nombre_actual_limpio[51];
    strncpy(nombre_actual_limpio, df_actual->columnas[i].nombre, 50);
    nombre_actual_limpio[50] = '\0';
    for (int j = 0; nombre_actual_limpio[j]; j++) {
      nombre_actual_limpio[j] = tolower(nombre_actual_limpio[j]);
    }

    if (strcmp(nombre_actual_limpio, nombre_col_limpio) == 0) {
      indice_col = i;
      break;
    }
  }

  if (indice_col == -1) {
    print_error("Columna no encontrada");
    return;
  }

  int nuevasColumnas = df_actual->numColumnas - 1;

  if (nuevasColumnas == 0) {
    print_error("No se puede eliminar la última columna");
    return;
  }

  Dataframe *nuevo_df = malloc(sizeof(Dataframe));
  if (!crearDF(nuevo_df, nuevasColumnas, df_actual->numFilas, df_actual->indice)) {
    print_error("Error al crear nuevo dataframe");
    free(nuevo_df);
    return;
  }

  int nuevaColIndex = 0;
  for (int i = 0; i < df_actual->numColumnas; i++) {
    if (i == indice_col) continue;

    strncpy(nuevo_df->columnas[nuevaColIndex].nombre, df_actual->columnas[i].nombre, 50);
    nuevo_df->columnas[nuevaColIndex].tipo = df_actual->columnas[i].tipo;

    for (int j = 0; j < df_actual->numFilas; j++) {
      nuevo_df->columnas[nuevaColIndex].datos[j] =
          df_actual->columnas[i].datos[j] ? strdup(df_actual->columnas[i].datos[j]) : NULL;
      nuevo_df->columnas[nuevaColIndex].esNulo[j] = df_actual->columnas[i].esNulo[j];
    }

    nuevaColIndex++;
  }

  liberarMemoriaDF(df_actual);
  df_actual = nuevo_df;

  snprintf(promptTerminal, sizeof(promptTerminal), "[%s: %d,%d]:> ",
           df_actual->indice, df_actual->numFilas, df_actual->numColumnas);

  printf(GREEN "Se eliminó la columna '%s'\n" RESET, nombre_col);
}

void delnullCLI(const char *nombre_col) {
  if (!df_actual || !nombre_col) {
    print_error("No hay df activo o nombre de columna inválido");
    return;
  }

  int indice_col = -1;
  for (int i = 0; i < df_actual->numColumnas; i++) {
    if (strcmp(df_actual->columnas[i].nombre, nombre_col) == 0) {
      indice_col = i;
      break;
    }
  }

  if (indice_col == -1) {
    print_error("Columna no encontrada");
    return;
  }

  int nullRows = 0;
  for (int i = 0; i < df_actual->numFilas; i++) {
    if (df_actual->columnas[indice_col].esNulo[i]) {
      nullRows++;
    }
  }

  if (nullRows == 0) {
    printf(GREEN "No hay valores nulos para eliminar\n" RESET);
    return;
  }

  int validRows = df_actual->numFilas - nullRows;

  Dataframe *nuevo_df = malloc(sizeof(Dataframe));
  if (!crearDF(nuevo_df, df_actual->numColumnas, validRows, df_actual->indice)) {
    print_error("Error al crear nuevo dataframe");
    free(nuevo_df);
    return;
  }

  for (int i = 0; i < df_actual->numColumnas; i++) {
    strncpy(nuevo_df->columnas[i].nombre, df_actual->columnas[i].nombre, 50);
    nuevo_df->columnas[i].tipo = df_actual->columnas[i].tipo;
  }

  int newRow = 0;
  for (int i = 0; i < df_actual->numFilas; i++) {
    if (!df_actual->columnas[indice_col].esNulo[i]) {
      for (int j = 0; j < df_actual->numColumnas; j++) {
        nuevo_df->columnas[j].datos[newRow] =
            df_actual->columnas[j].datos[i] ? strdup(df_actual->columnas[j].datos[i]) : NULL;
        nuevo_df->columnas[j].esNulo[newRow] = df_actual->columnas[j].esNulo[i];
      }
      newRow++;
    }
  }

  liberarMemoriaDF(df_actual);
  df_actual = nuevo_df;

  snprintf(promptTerminal, sizeof(promptTerminal), "[%s: %d,%d]:> ",
           df_actual->indice, df_actual->numFilas, df_actual->numColumnas);

  printf(GREEN "Se eliminaron %d filas con valores nulos\n" RESET, nullRows);
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

  while ((read = getline(&line, &len, file)) != -1 && fila_actual < numFilas) {
    verificarNulos(line, fila_actual, df);

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
    df->columnas[i].esNulo = calloc(numFilas, sizeof(int));

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
  if (!df || numColumnas <= 0 || numFilas <= 0 || !nombre_df) {
    print_error("Invalid parameters");
    return 0;
  }

  df->columnas = calloc(numColumnas, sizeof(Columna));
  if (!df->columnas) {
    print_error("Memory allocation failed");
    return 0;
  }

  for (int i = 0; i < numColumnas; i++) {
    df->columnas[i].datos = calloc(numFilas, sizeof(char *));
    df->columnas[i].esNulo = calloc(numFilas, sizeof(int));

    if (!df->columnas[i].datos || !df->columnas[i].esNulo) {
      for (int j = 0; j < i; j++) {
        free(df->columnas[j].datos);
        free(df->columnas[j].esNulo);
      }
      free(df->columnas);
      print_error("Memory allocation failed");
      return 0;
    }
  }

  df->numColumnas = numColumnas;
  df->numFilas = numFilas;
  strncpy(df->indice, nombre_df, sizeof(df->indice) - 1);
  df->indice[sizeof(df->indice) - 1] = '\0';

  return 1;
}

void loadearCSV(const char *nombre_archivo) {
  if (!nombre_archivo) {
    print_error("Nombre de archivo inválido");
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

  int numColumnas = contarColumnas(headerLine);
  if (numColumnas > MAX_COLUMNS) {
    print_error("Demasiadas columnas");
    free(headerLine);
    fclose(file);
    return;
  }

  const int BATCH_SIZE = 1000;
  char nombre_df[10];
  snprintf(nombre_df, sizeof(nombre_df), "df%d", listaDF.numDFs);

  Dataframe *nuevo_df = malloc(sizeof(Dataframe));
  if (!crearDF(nuevo_df, numColumnas, BATCH_SIZE, nombre_df)) {
    print_error("Error al crear dataframe");
    free(headerLine);
    fclose(file);
    return;
  }

  char *token = strtok(headerLine, ",\n\r");
  int col = 0;
  while (token && col < numColumnas) {
    strncpy(nuevo_df->columnas[col].nombre, token, sizeof(nuevo_df->columnas[col].nombre) - 1);
    col++;
    token = strtok(NULL, ",\n\r");
  }

  free(headerLine);

  procesarPorLotes(file, nuevo_df, BATCH_SIZE);
  fclose(file);

  if (nuevo_df->numFilas > 0) {
    for (int i = 0; i < nuevo_df->numColumnas; i++) {
      nuevo_df->columnas[i].datos = realloc(nuevo_df->columnas[i].datos, nuevo_df->numFilas * sizeof(char *));
      nuevo_df->columnas[i].esNulo = realloc(nuevo_df->columnas[i].esNulo, nuevo_df->numFilas * sizeof(int));
    }
  }

  df_actual = nuevo_df;
  agregarDF(nuevo_df);
  listaDF.numDFs++;

  tiposColumnas(df_actual);

  snprintf(promptTerminal, sizeof(promptTerminal), "[%s: %d,%d]:> ",
           df_actual->indice, df_actual->numFilas, df_actual->numColumnas);

  printf(GREEN "Archivo cargado: %d filas, %d columnas\n" RESET,
         df_actual->numFilas, df_actual->numColumnas);
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
  struct tm tm;
  memset(&tm, 0, sizeof(struct tm));

  if (sscanf(str_fecha, "%4d-%2d-%2d", &tm.tm_year, &tm.tm_mon, &tm.tm_mday) != 3) {
    return 0;
  }

  tm.tm_year -= 1900;
  tm.tm_mon -= 1;
  time_t t = mktime(&tm);
  return t != -1;
}

void tiposColumnas(Dataframe *df) {
  for (int col = 0; col < df->numColumnas; col++) {
    df->columnas[col].tipo = TEXTO;
    int es_numerica = 1;
    int es_fecha = 1;

    for (int row = 0; row < df->numFilas; row++) {
      char *value = (char *)df->columnas[col].datos[row];
      if (value == NULL) continue;

      char *endptr;
      strtod(value, &endptr);
      if (endptr == value) {
        es_numerica = 0;
      }

      if (!fechaValida(value)) {
        es_fecha = 0;
      }
    }

    if (es_fecha) {
      df->columnas[col].tipo = FECHA;
    } else if (es_numerica) {
      df->columnas[col].tipo = NUMERICO;
    }
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

  df_actual = nodoActual->df;
  snprintf(promptTerminal, sizeof(promptTerminal), "[%s: %d,%d]:> ",
           df_actual->indice, df_actual->numFilas, df_actual->numColumnas);
  printf(GREEN "Cambiado a %s con %d filas y %d columnas\n" RESET,
         nodoActual->df->indice, nodoActual->df->numFilas,
         nodoActual->df->numColumnas);
}
void saveCLI(const char *nombre_archivo) {
  if (!df_actual) {
    print_error("No hay df activo para guardar.");
    return;
  }

  char nombre_saveCLI[MAX_FILENAME];
  if (nombre_archivo == NULL || strlen(nombre_archivo) == 0) {
    snprintf(nombre_saveCLI, sizeof(nombre_saveCLI), "%s.csv",
             df_actual->indice);
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

  for (int col = 0; col < df_actual->numColumnas; col++) {
    fprintf(file, "%s", df_actual->columnas[col].nombre);
    if (col < df_actual->numColumnas - 1) {
      fprintf(file, ",");
    }
  }
  fprintf(file, "\n");

  for (int row = 0; row < df_actual->numFilas; row++) {
    for (int col = 0; col < df_actual->numColumnas; col++) {
      if (df_actual->columnas[col].esNulo[row]) {
      } else if (df_actual->columnas[col].datos[row] != NULL) {
        char *valor = (char *)df_actual->columnas[col].datos[row];
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

      if (col < df_actual->numColumnas - 1) {
        fprintf(file, ",");
      }
    }
    fprintf(file, "\n");
  }

  fclose(file);
  printf(GREEN "df guardado exitosamente en %s\n" RESET, nombre_saveCLI);
}
void metaCLI() {
  if (!df_actual) {
    print_error("No hay df cargado.");
    return;
  }

  for (int col = 0; col < df_actual->numColumnas; col++) {
    int contador_nulos = 0;

    for (int row = 0; row < df_actual->numFilas; row++) {
      if (df_actual->columnas[col].esNulo[row]) {
        contador_nulos++;
      }
    }

    char *tipo;
    switch (df_actual->columnas[col].tipo) {
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
           df_actual->columnas[col].nombre, tipo, contador_nulos);
  }
}

void viewCLI(int n) {
  if (!df_actual) {
    print_error("No hay df cargado.");
    return;
  }

  for (int j = 0; j < df_actual->numColumnas; j++) {
    printf("%s", df_actual->columnas[j].nombre);

    if (j < df_actual->numColumnas - 1) {
      printf(",");
    }
  }

  printf("\n");

  int filas_a_mostrar = (n < df_actual->numFilas) ? n : df_actual->numFilas;

  for (int i = 0; i < filas_a_mostrar; i++) {
    for (int j = 0; j < df_actual->numColumnas; j++) {
      if (df_actual->columnas[j].esNulo[i]) {
        printf("1");

      } else {
        if (df_actual->columnas[j].datos[i] == NULL) {
          printf("Nulo");

        } else {
          printf("%s", (char *)df_actual->columnas[j].datos[i]);
        }
      }

      if (j < df_actual->numColumnas - 1) {
        printf(",");
      }
    }

    printf("\n");
  }
}

int encontrarIndiceColumna(Dataframe *df, const char *nombre_columna) {
  for (int i = 0; i < df->numColumnas; i++) {
    if (strcmp(df->columnas[i].nombre, nombre_columna) == 0) {
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

void sortCLI(const char *nombre_columna, int descendente) {
  if (!df_actual) {
    print_error("No hay df cargado.");
    return;
  }

  int indice_columna = encontrarIndiceColumna(df_actual, nombre_columna);
  if (indice_columna == -1) {
    print_error("Columna no encontrada.");
    return;
  }

  ordenarDataframe(df_actual, indice_columna, descendente);

  printf(GREEN "df ordenado por columna '%s' en orden %s.\n" RESET,
         nombre_columna, descendente ? "descendente" : "ascendente");
}

void verificarNulos(char *lineaLeida, int fila, Dataframe *df) {
  cortarEspacios(lineaLeida);

  char resultado[MAX_LINE_LENGTH * 2] = {0};

  int j = 0;

  int longitud = strlen(lineaLeida);

  int esperando_valor = 1;

  int indice_columna = 0;

  for (int i = 0; i <= longitud; i++) {
    if (i == longitud || lineaLeida[i] == ',' || lineaLeida[i] == '\r') {
      if (esperando_valor) {
        resultado[j++] = '1';

        df->columnas[indice_columna].esNulo[fila] = 1;
      }

      if (i < longitud) {
        resultado[j++] = lineaLeida[i];
      }

      esperando_valor = 1;

      if (lineaLeida[i] == '\n' || lineaLeida[i] == '\r') {
        resultado[j++] = '\r';

        resultado[j++] = '\n';
      }

      indice_columna++;

    } else {
      resultado[j++] = lineaLeida[i];

      esperando_valor = 0;
    }
  }

  resultado[j] = '\0';

  strcpy(lineaLeida, resultado);
}

