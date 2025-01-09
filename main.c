#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// Constantes de configuración del sistema
#define MAX_LINE_LENGTH 4096
#define MAX_COLUMNS 100
#define MAX_ROWS 5000
#define MAX_FILENAME 256

// Códigos de color ANSI para salida por consola
#define RED "\x1b[31m"
#define GREEN "\x1b[32m"
#define WHITE "\x1b[37m"
#define RESET "\x1b[0m"

// Enumeración de tipos de datos soportados
typedef enum {
  TEXTO,    // Tipo cadena de texto
  NUMERICO, // Tipo numérico (punto flotante)
  FECHA     // Tipo fecha
} TipoDato;

// Estructura de Columna: Representa una columna en el df
typedef struct {
  char nombre[50];      // Nombre de la columna
  TipoDato tipo;        // Tipo de datos de la columna
  char **datos;         // Arreglo de punteros a datos
  unsigned int *esNulo; // Indicador de valores nulos
  int numFilas;         // Número de filas en la columna
} Columna;

// Estructura de df: Contenedor principal de datos
typedef struct {
  Columna *columnas; // Arreglo de columnas
  int numColumnas;   // Número total de columnas
  int numFilas;      // Número total de filas
  char indice[20];   // Nombre del df
} Dataframe;

// Alias para tipos FECHA: 'Fecha' alias de 'struct tm' (#include <time.h>)
typedef struct tm Fecha;

// Estructura para representar un nodo de la lista
typedef struct NodoLista {
  Dataframe *df;               // puntero a Dataframe
  struct NodoLista *siguiente; // Puntero al siguiente nodo de la lista
} Nodo;

// Estructura para representar la lista de Dataframe’s
typedef struct {
  int numDFs;    // Número de Dataframes almacenados en la lista
  Nodo *primero; // Puntero al primer Nodo de la lista
} Lista;

// Variables globales para gestión del sistema
Lista listaDF;               // Global variable to store all dataframes
Dataframe *df_actual = NULL; // Variable global que apunta al dataframe activo
char promptTerminal[MAX_LINE_LENGTH] =
    "[?]:> "; // Prompt de la interfaz interactiva

// Prototipos de todas las funciones utilizadas
// Funciones de inicialización y gestión
void inicializarLista();
void CLI();
void print_error(const char *mensaje_error);

// Funciones de carga y creación de dataframes
void contarFilasYColumnas(const char *nombre_archivo, int *numFilas,
                          int *numColumnas);
void loadearCSV(const char *nombre_archivo);
int crearDF(Dataframe *df, int numColumnas, int numFilas,
                    const char *nombre_df);
void liberarMemoriaDF(Dataframe *df);

// Funciones de procesamiento de datos
int contarColumnas(const char *line);
void cortarEspacios(char *str);
void leerEncabezados(FILE *file, Dataframe *df, int numColumnas);
void leerFilas(FILE *file, Dataframe *df, int numFilas, int numColumnas);

// Funciones de manipulación de dataframes
void agregarDF(Dataframe *nuevoDF);
void cambiarDF(Lista *lista, int indice);

// Funciones de interfaz de usuario
void metaCLI();
void viewCLI(int n);
void sortCLI(const char *nombre_col, int esta_desc);
void saveCLI(const char *nombre_archivo);

// Funciones de procesamiento y validación
void verificarNulos(char *line, int fila, Dataframe *df);
int fechaValida(const char *str_fecha);
int compararValores(void *a, void *b, TipoDato tipo, int esta_desc);
void tiposColumnas(Dataframe *df);

// Funciones de filtrado y manipulación de datos
void filterCLI(const char *nombre_col, const char *operator,
               const char * value);
void delnullCLI(const char *nombre_col);
void liberarListaCompleta(Lista *lista);

int main() {
  inicializarLista(); // Inicializar la lista al inicio
  // Mostrar mensaje de bienvenida con color verde
  printf(GREEN "Sistema de Gestión de Dataframes\n" RESET);

  // Iniciar interfaz de línea de comandos interactiva
  CLI();
  liberarListaCompleta(&listaDF); // Add this line
  return 0;
}

void liberarListaCompleta(Lista *lista) {
    Nodo *actual = listaDF.primero;
    while (actual) {
        Nodo *siguiente = actual->siguiente;
        liberarMemoriaDF(actual->df);
        free(actual);
        actual = siguiente;
    }
    listaDF.primero = NULL;
    listaDF.numDFs = 0;
}

void liberarMemoriaDF(Dataframe *df) {
    if (!df) return;
    
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
    // Mostrar el prompt del terminal y leer el input del usuario
    printf("%s", promptTerminal);
    fgets(input, sizeof(input), stdin);
    cortarEspacios(input); // Eliminar espacios en blanco al inicio y final del input

    // Comando "quit": salir del CLI
    if (strcmp(input, "quit") == 0) {
      break;
    }

    // Comando "load": cargar un archivo CSV
    else if (strncmp(input, "load ", 5) == 0) {
      loadearCSV(input + 5); // Cargar el CSV especificado después de "load"
    }

    // Comando "meta": mostrar metadatos
    else if (strcmp(input, "meta") == 0) {
      metaCLI();
    }

    // Comando "save": guardar datos
    else if (strncmp(input, "save", 4) == 0) {
      saveCLI(input + 4); // Pasar argumentos después de "save"
    }

    // Comando "view": mostrar filas de datos
    else if (strncmp(input, "view", 4) == 0) {
      int n = 10; // Por defecto, mostrar 10 filas

      // Verificar si el usuario especificó un número de filas
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

    // Comando "sort": ordenar los datos
    else if (strncmp(input, "sort ", 5) == 0) {
      char nombre_col[50];
      char order[10] = "asc"; // Orden ascendente por defecto
      int esta_desc = 0;

      // Parsear el nombre de la columna y el orden
      int parsed = sscanf(input + 5, "%49s %9s", nombre_col, order);
      if (parsed == 0) {
        print_error("Comando de ordenamiento inválido");
        continue;
      }

      // Verificar el orden especificado (asc o des)
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

    // Comando "delnull": eliminar valores nulos
    else if (strncmp(input, "delnull ", 8) == 0) {
      delnullCLI(input + 8); // Pasar argumentos después de "delnull"
    }

    // Comando "df": cambiar de dataframe
    else if (strncmp(input, "df", 2) == 0 && strlen(input) > 2) {
    char *endptr;
    long indice = strtol(input + 2, &endptr, 10);
    
    if (*endptr == '\0' && indice >= 0 && indice < listaDF.numDFs) {
        cambiarDF(&listaDF, indice);
    } else {
        print_error("Índice de dataframe inválido.");
    }
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

void delnullCLI(const char *nombre_col) {
  if (!df_actual) {
    print_error("No hay df cargado.");
    return;
  }

  // Encontrar la columna por nombre
  int indice_col = -1;
  for (int i = 0; i < df_actual->numColumnas; i++) {
    if (strcmp(df_actual->columnas[i].nombre, nombre_col) == 0) {
      indice_col = i;
      break;
    }
  }
  if (indice_col == -1) {
    print_error("Columna no encontrada.");
    return;
  }

  // Contar filas con valores nulos en la columna especificada
  int null_count = 0;
  for (int i = 0; i < df_actual->numFilas; i++) {
    if (df_actual->columnas[indice_col].esNulo[i]) {
      null_count++;
    }
  }

  if (null_count == 0) {
    printf(GREEN "No se encontraron valores nulos en la columna '%s'.\n" RESET,
           nombre_col);
    return;
  }

  // Crear un nuevo dataframe sin las filas nulas
  Dataframe *new_df = malloc(sizeof(Dataframe));
  if (!new_df) {
    print_error("Fallo en la asignación de memoria para el nuevo dataframe.");
    return;
  }

  // Inicializar el nuevo dataframe
  crearDF(new_df, df_actual->numColumnas,
                 df_actual->numFilas - null_count, df_actual->indice);

  // Copiar los encabezados
  for (int i = 0; i < df_actual->numColumnas; i++) {
    strncpy(new_df->columnas[i].nombre, df_actual->columnas[i].nombre, 50);
    new_df->columnas[i].tipo = df_actual->columnas[i].tipo;
  }

  // Copiar las filas que no tienen valores nulos en la columna especificada
  int new_row = 0;
  for (int i = 0; i < df_actual->numFilas; i++) {
    if (!df_actual->columnas[indice_col].esNulo[i]) {
      for (int j = 0; j < df_actual->numColumnas; j++) {
        if (df_actual->columnas[j].datos[i]) {
          new_df->columnas[j].datos[new_row] =
              strdup((char *)df_actual->columnas[j].datos[i]);
          if (!new_df->columnas[j].datos[new_row]) {
            print_error("Fallo en la asignación de memoria para los datos.");
            liberarMemoriaDF(new_df);
            return;
          }
        } else {
          new_df->columnas[j].datos[new_row] = NULL;
        }
        new_df->columnas[j].esNulo[new_row] = df_actual->columnas[j].esNulo[i];
      }
      new_row++;
    }
  }

  // Liberar el dataframe actual y asignar el nuevo dataframe
  liberarMemoriaDF(df_actual);
  df_actual = new_df;

  // Actualizar el prompt con el nuevo número de filas
  snprintf(promptTerminal, sizeof(promptTerminal), "[%s: %d,%d]:> ",
           df_actual->indice, df_actual->numFilas, df_actual->numColumnas);

  printf(GREEN
         "Se eliminaron %d filas con valores nulos en la columna '%s'.\n" RESET,
         null_count, nombre_col);
}

void filterCLI(const char *nombre_col, const char *operator,
               const char * value) {
  if (!df_actual) {
    print_error("No hay df cargado.");
    return;
  }

  // Encontrar la columna por nombre
  int indice_col = -1;
  for (int i = 0; i < df_actual->numColumnas; i++) {
    if (strcmp(df_actual->columnas[i].nombre, nombre_col) == 0) {
      indice_col = i;
      break;
    }
  }
  if (indice_col == -1) {
    print_error("Columna no encontrada.");
    return;
  }

  // Determinar el tipo de la columna
  TipoDato tipo_col = df_actual->columnas[indice_col].tipo;

  // Crear un nuevo dataframe para almacenar las filas filtradas
  Dataframe *df_filtrado = malloc(sizeof(Dataframe));
  if (!df_filtrado) {
    print_error(
        "Fallo en la asignación de memoria para el dataframe filtrado.");
    return;
  }

  // Inicializar el dataframe filtrado
  crearDF(df_filtrado, df_actual->numColumnas, df_actual->numFilas,
                 "df_filtrado");

  // Copiar los encabezados
  for (int i = 0; i < df_actual->numColumnas; i++) {
    strncpy(df_filtrado->columnas[i].nombre, df_actual->columnas[i].nombre, 50);
    df_filtrado->columnas[i].tipo = df_actual->columnas[i].tipo;
  }

  // Filtrar las filas
  int filas_filtradas = 0;
  for (int i = 0; i < df_actual->numFilas; i++) {
    int keep_row = 0;

    if (df_actual->columnas[indice_col].esNulo[i]) {
      // Si el valor es nulo, no se incluye en el filtrado
      continue;
    }

    if (tipo_col == NUMERICO) { // Tipo numérico
      double col_value = atof((char *)df_actual->columnas[indice_col].datos[i]);
      double filter_value = atof(value);

      if (strcmp(operator, "eq") == 0 && col_value == filter_value)
        keep_row = 1;
      else if (strcmp(operator, "neq") == 0 && col_value != filter_value)
        keep_row = 1;
      else if (strcmp(operator, "gt") == 0 && col_value > filter_value)
        keep_row = 1;
      else if (strcmp(operator, "lt") == 0 && col_value < filter_value)
        keep_row = 1;
    } else if (tipo_col == TEXTO) { // Tipo texto
      char *col_value = (char *)df_actual->columnas[indice_col].datos[i];
      char *filter_value = (char *)value;

      if (strcmp(operator, "eq") == 0 && strcmp(col_value, filter_value) == 0)
        keep_row = 1;
      else if (strcmp(operator, "neq") == 0 &&
               strcmp(col_value, filter_value) != 0)
        keep_row = 1;
    } else if (tipo_col == FECHA) { // Tipo fecha
      struct tm tm_col = {0}, tm_filter = {0};
      sscanf((char *)df_actual->columnas[indice_col].datos[i], "%4d-%2d-%2d",
             &tm_col.tm_year, &tm_col.tm_mon, &tm_col.tm_mday);
      sscanf(value, "%4d-%2d-%2d", &tm_filter.tm_year, &tm_filter.tm_mon,
             &tm_filter.tm_mday);
      tm_col.tm_year -= 1900;
      tm_filter.tm_year -= 1900;
      tm_col.tm_mon -= 1;
      tm_filter.tm_mon -= 1;

      time_t time_col = mktime(&tm_col);
      time_t time_filter = mktime(&tm_filter);

      if (strcmp(operator, "eq") == 0 && time_col == time_filter)
        keep_row = 1;
      else if (strcmp(operator, "neq") == 0 && time_col != time_filter)
        keep_row = 1;
      else if (strcmp(operator, "gt") == 0 && time_col > time_filter)
        keep_row = 1;
      else if (strcmp(operator, "lt") == 0 && time_col < time_filter)
        keep_row = 1;
    }

    if (keep_row) {
      // Copiar la fila al nuevo dataframe
      for (int j = 0; j < df_actual->numColumnas; j++) {
        if (df_actual->columnas[j].datos[i]) {
          df_filtrado->columnas[j].datos[filas_filtradas] =
              strdup((char *)df_actual->columnas[j].datos[i]);
          if (!df_filtrado->columnas[j].datos[filas_filtradas]) {
            print_error(
                "Fallo en la asignación de memoria para los datos filtrados.");
            liberarMemoriaDF(df_filtrado);
            return;
          }
        } else {
          df_filtrado->columnas[j].datos[filas_filtradas] = NULL;
        }
        df_filtrado->columnas[j].esNulo[filas_filtradas] =
            df_actual->columnas[j].esNulo[i];
      }
      filas_filtradas++;
    }
  }

  // Actualizar el número de filas en el dataframe filtrado
  df_filtrado->numFilas = filas_filtradas;

  // Liberar el dataframe actual y asignar el nuevo dataframe filtrado
  liberarMemoriaDF(df_actual);
  df_actual = df_filtrado;

  printf(GREEN "df filtrado exitosamente. Filas restantes: %d\n" RESET,
         filas_filtradas);
}

void contarFilasYColumnas(const char *nombre_archivo, int *numFilas,
                          int *numColumnas) {
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
      strncpy(df->columnas[col].nombre, token,
              sizeof(df->columnas[col].nombre) - 1);
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

    // Verify memory requirements aren't excessive
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
            // Cleanup on failure
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
    
    return 1;    if (!df || numColumnas <= 0 || numFilas <= 0 || !nombre_df) {
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
  int numFilas = 0;
  int numColumnas = 0;

  contarFilasYColumnas(nombre_archivo, &numFilas, &numColumnas);

  if (numFilas == 0 || numColumnas == 0) {
    print_error("Archivo vacío o inválido");
    return;
  }

  FILE *file = fopen(nombre_archivo, "r");
  if (!file) {
    print_error("No se puede abrir el archivo");
    return;
  }

  char nombre_df[10];
  snprintf(nombre_df, sizeof(nombre_df), "df%d", listaDF.numDFs);
  listaDF.numDFs++;

  df_actual = malloc(sizeof(Dataframe));
  if (!df_actual) {
    print_error("Fallo en asignación de memoria para df");
    fclose(file);
    return;
  }

  crearDF(df_actual, numColumnas, numFilas, nombre_df);

  leerEncabezados(file, df_actual, numColumnas);

  leerFilas(file, df_actual, numFilas, numColumnas);

  fclose(file);

  agregarDF(df_actual);

  tiposColumnas(df_actual);

  snprintf(promptTerminal, sizeof(promptTerminal), "[%s: %d,%d]:> ",
           df_actual->indice, df_actual->numFilas, df_actual->numColumnas);

  printf(GREEN "Cargado exitosamente %s con %d filas y %d columnas\n" RESET,
         nombre_archivo, df_actual->numFilas, df_actual->numColumnas);
}

void cortarEspacios(char *str) {
  size_t len = strlen(str);
  if (len > 0 && str[len - 1] == '\n') {
    str[len - 1] = '\0'; // Elimina el salto de línea al final
  }

  // Eliminar espacios en blanco al inicio de la cadena
  char *start = str;
  while (*start == ' ') {
    start++;
  }

  // Mover la cadena procesada al inicio
  if (start != str) {
    memmove(str, start, strlen(start) + 1);
  }

  // Eliminar espacios en blanco al final
  len = strlen(str);
  while (len > 0 && str[len - 1] == ' ') {
    str[--len] = '\0';
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
  struct tm tm;
  memset(&tm, 0, sizeof(struct tm));

  // Parsear fecha compatible con diferentes sistemas
  if (sscanf(str_fecha, "%4d-%2d-%2d", &tm.tm_year, &tm.tm_mon, &tm.tm_mday) !=
      3) {
    return 0;
  }

  tm.tm_year -= 1900; // Ajustar año desde 1900
  tm.tm_mon -= 1;     // Ajustar mes (0-11)
  time_t t = mktime(&tm);
  return t != -1;
}

void tiposColumnas(Dataframe *df) {
  for (int col = 0; col < df->numColumnas; col++) {
    df->columnas[col].tipo = TEXTO; // Tipo por defecto
    int is_numeric = 1;
    int is_date = 1;

    for (int row = 0; row < df->numFilas; row++) {
      char *value = (char *)df->columnas[col].datos[row];
      if (value == NULL)
        continue;

      // Verificar si es numérico
      char *endptr;
      strtod(value, &endptr);
      if (endptr == value) {
        is_numeric = 0;
      }

      // Verificar si es fecha
      if (!fechaValida(value)) {
        is_date = 0;
      }
    }

    // Establecer tipo con prioridad: Fecha > Numérico > Texto
    if (is_date) {
      df->columnas[col].tipo = FECHA;
    } else if (is_numeric) {
      df->columnas[col].tipo = NUMERICO;
    }
  }
}

int compararValores(void *a, void *b, TipoDato tipo, int esta_desc) {
  // Manejar casos de valores nulos
  if (a == NULL && b == NULL)
    return 0;
  if (a == NULL)
    return esta_desc ? 1 : -1;
  if (b == NULL)
    return esta_desc ? -1 : 1;

  char *str_a = (char *)a;
  char *str_b = (char *)b;

  switch (tipo) {
  // Comparación de valores numéricos
  case NUMERICO: {
    double num_a = atof(str_a);
    double num_b = atof(str_b);
    return esta_desc ? (num_a < num_b ? 1 : (num_a > num_b ? -1 : 0))
                         : (num_a < num_b ? -1 : (num_a > num_b ? 1 : 0));
  }
  // Comparación de fechas
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
  // Comparación de cadenas de texto
  case TEXTO: {
    return esta_desc ? strcmp(str_b, str_a) : strcmp(str_a, str_b);
  }
  default:
    return 0;
  }
}

void print_error(const char *mensaje_error) {
  // Imprime mensajes de error en color rojo
  fprintf(stderr, RED "ERROR: %s\n" RESET, mensaje_error);
}

void inicializarLista() {
  listaDF.numDFs = 0;
  listaDF.primero = NULL;
}

void cambiarDF(Lista *lista, int index) {
    if (index < 0 || index >= lista->numDFs) {
        print_error("Índice de dataframe inválido.");
        return;
    }

    int reverseIndex = lista->numDFs - 1 - index;  // Reverse the index
    Nodo *nodoActual = lista->primero;
    
    for (int i = 0; i < reverseIndex; i++) {
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
        nodoActual->df->indice, nodoActual->df->numFilas, nodoActual->df->numColumnas);
}

void saveCLI(const char *nombre_archivo) {
  if (!df_actual) {
    print_error("No hay df activo para guardar.");
    return;
  }

  // If no nombre_archivo was provided, create a default one
  char nombre_saveCLI[MAX_FILENAME];
  if (nombre_archivo == NULL || strlen(nombre_archivo) == 0) {
    snprintf(nombre_saveCLI, sizeof(nombre_saveCLI), "%s.csv", df_actual->indice);
  } else {
    strncpy(nombre_saveCLI, nombre_archivo, sizeof(nombre_saveCLI) - 1);
    nombre_saveCLI[sizeof(nombre_saveCLI) - 1] = '\0';
    cortarEspacios(nombre_saveCLI);
  }

  // Add .csv extension if not present
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

  // Write headers
  for (int col = 0; col < df_actual->numColumnas; col++) {
    fprintf(file, "%s", df_actual->columnas[col].nombre);
    if (col < df_actual->numColumnas - 1) {
      fprintf(file, ",");
    }
  }
  fprintf(file, "\n");

  // Write data
  for (int row = 0; row < df_actual->numFilas; row++) {
    for (int col = 0; col < df_actual->numColumnas; col++) {
      if (df_actual->columnas[col].esNulo[row]) {
        // Write nothing for null values
      } else if (df_actual->columnas[col].datos[row] != NULL) {
        char *valor = (char *)df_actual->columnas[col].datos[row];
        // Check if value needs quoting
        int needs_quotes =
            (strchr(valor, ',') != NULL || strchr(valor, '"') != NULL);

        if (needs_quotes) {
          fprintf(file, "\"");
          for (char *c = valor; *c; c++) {
            if (*c == '"') {
              fprintf(file, "\"\""); // Double quotes for escaping
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
    int null_count = 0;

    for (int row = 0; row < df_actual->numFilas; row++) {
      if (df_actual->columnas[col].esNulo[row]) {
        null_count++;
      }
    }

    char *type_str;
    switch (df_actual->columnas[col].tipo) {
    case TEXTO:
      type_str = "Texto";
      break;
    case NUMERICO:
      type_str = "Numérico";
      break;
    case FECHA:
      type_str = "Fecha";
      break;
    default:
      type_str = "Desconocido";
      break;
    }

    printf(GREEN "%s: %s (Valores nulos: %d)\n" RESET,
           df_actual->columnas[col].nombre, type_str, null_count);
  }
}

void viewCLI(int n) {
  if (!df_actual) {
    print_error("No hay df cargado.");
    return;
  }

  printf(WHITE "                    df: %s\n\n" RESET, df_actual->indice);

  // Mostrar encabezados
  for (int j = 0; j < df_actual->numColumnas; j++) {
    printf("%s", df_actual->columnas[j].nombre);
    if (j < df_actual->numColumnas - 1) {
      printf(",");
    }
  }
  printf("\n");

  // Determinar número de filas a mostrar
  int rows_to_show = (n < df_actual->numFilas) ? n : df_actual->numFilas;

  // Mostrar filas de datos
  for (int i = 0; i < rows_to_show; i++) {
    for (int j = 0; j < df_actual->numColumnas; j++) {
      if (df_actual->columnas[j].esNulo[i]) {
        printf("1");
      } else {
        if (df_actual->columnas[j].datos[i] == NULL) {
          printf("");
        } else {
          printf("%s", (char *)df_actual->columnas[j].datos[i]);
        }
      }
      // Agregar coma si no es la última columna
      if (j < df_actual->numColumnas - 1) {
        printf(",");
      }
    }
    printf("\n");
  }
}

void sortCLI(const char *nombre_col, int esta_desc) {
  if (!df_actual) {
    print_error("No hay df cargado.");
    return;
  }

  int column_index = -1;
  for (int i = 0; i < df_actual->numColumnas; i++) {
    if (strcmp(df_actual->columnas[i].nombre, nombre_col) == 0) {
      column_index = i;
      break;
    }
  }

  if (column_index == -1) {
    print_error("Columna no encontrada.");
    return;
  }

  TipoDato column_type = df_actual->columnas[column_index].tipo;

  // Algoritmo de ordenamiento de burbuja
  for (int i = 0; i < df_actual->numFilas - 1; i++) {
    for (int j = 0; j < df_actual->numFilas - i - 1; j++) {
      void *val1 = df_actual->columnas[column_index].datos[j];
      void *val2 = df_actual->columnas[column_index].datos[j + 1];

      if (compararValores(val1, val2, column_type, esta_desc) > 0) {
        // Intercambiar datos de todas las columnas
        for (int col = 0; col < df_actual->numColumnas; col++) {
          void *temp_data = df_actual->columnas[col].datos[j];
          df_actual->columnas[col].datos[j] =
              df_actual->columnas[col].datos[j + 1];
          df_actual->columnas[col].datos[j + 1] = temp_data;

          // Intercambiar valores de nulidad
          int temp_null = df_actual->columnas[col].esNulo[j];
          df_actual->columnas[col].esNulo[j] =
              df_actual->columnas[col].esNulo[j + 1];
          df_actual->columnas[col].esNulo[j + 1] = temp_null;
        }
      }
    }
  }

  printf(GREEN "df ordenado por columna '%s' en orden %s.\n" RESET, nombre_col,
         esta_desc ? "descendente" : "ascendente");
}

void verificarNulos(char *linea, int fila, Dataframe *df) {
  cortarEspacios(linea);

  char resultado[MAX_LINE_LENGTH * 2] = {0};
  int j = 0;
  int length = strlen(linea);
  int expecting_value = 1;

  int indice_col = 0;

  for (int i = 0; i <= length; i++) {
    if (i == length || linea[i] == ',' || linea[i] == '\r') {
      if (expecting_value) {
        resultado[j++] = '1';
        df->columnas[indice_col].esNulo[fila] = 1;
      }

      if (i < length) {
        resultado[j++] = linea[i];
      }

      expecting_value = 1;

      if (linea[i] == '\n' || linea[i] == '\r') {
        resultado[j++] = '\r';
        resultado[j++] = '\n';
      }

      indice_col++;
    } else {
      resultado[j++] = linea[i];
      expecting_value = 0;
    }
  }

  resultado[j] = '\0';
  strcpy(linea, resultado);
}