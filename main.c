// Sistema de Gestión de Dataframes en C
// Objetivo: Cargar, manipular y analizar archivos CSV con funcionalidades avanzadas

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>  // Módulo para parseo de fechas

// Constantes de configuración del sistema
#define MAX_LINE_LENGTH 1024   // Longitud máxima permitida para una línea
#define MAX_COLUMNS 100         // Número máximo de columnas en un DataFrame
#define MAX_FILENAME 256        // Longitud máxima del nombre de archivo

// Códigos de color ANSI para salida por consola
#define RED     "\x1b[31m"     // Rojo para mensajes de error
#define GREEN   "\x1b[32m"     // Verde para mensajes de éxito
#define WHITE  "\x1b[37m"     // Blanco para texto general
#define RESET   "\x1b[0m"      // Resetear formato de color

// Enumeración de tipos de datos soportados
typedef enum {
    TEXTO,      // Tipo cadena de texto
    NUMERICO,   // Tipo numérico (punto flotante)
    FECHA       // Tipo fecha
} TipoDato;

// Estructura de Columna: Representa una columna en el DataFrame
typedef struct {
    char nombre[50];          // Nombre de la columna
    TipoDato tipo;          // Tipo de datos de la columna
    void **datos;            // Arreglo de punteros a datos
    int *esNulo;           // Indicador de valores nulos
    int numFilas;          // Número de filas en la columna
} Columna;

// Estructura de DataFrame: Contenedor principal de datos
typedef struct {
    Columna *columnas;        // Arreglo de columnas
    int numColumnas;       // Número total de columnas
    int numFilas;          // Número total de filas
    char indice[20];          // Nombre del DataFrame
} DataFrame;

// Alias para tipos FECHA: 'Fecha' alias de 'struct tm' (#include <time.h>)
typedef struct tm Fecha;

// Estructura para representar un nodo de la lista
typedef struct NodoLista{
    struct NodoLista *siguiente; // Puntero al siguiente nodo de la lista
} Nodo;
// Estructura para representar la lista de Dataframe’s
typedef struct {
    int numDFs; // Número de dataframes almacenados en la lista
    Nodo *primero; // Puntero al primer Nodo de la lista
} Lista;

// Variables globales para gestión del sistema
DataFrame *active_dataframe = NULL;  // DataFrame activo actualmente
char current_prompt[MAX_LINE_LENGTH] = "[?]:> ";  // Prompt de la interfaz interactiva

// Prototipos de todas las funciones utilizadas
void crearDataframe(DataFrame *df, int columnas, int rows, const char *nombre);
void liberarMemoriaDF(DataFrame *df);
int contarColumnas(const char *line);
void cortarEspacios(char *str);
void cargarCSV(const char *filename);
void CLI();
void print_error(const char *message);
void tiposColumnas(DataFrame *df);
void verificarNulos(char *linea);
int fechaValida(const char *date_str);
int compararValores(void *a, void *b, TipoDato tipo, int is_descending);
void metaCLI();
void viewCLI(int n);
void sortCLI(const char *column_name, int is_descending);
void showCLI();


// Función principal: Punto de entrada del programa
int main() {
    // Mostrar mensaje de bienvenida con color verde
    printf(GREEN "Sistema de Gestión de Dataframes\n" RESET);
    
    // Iniciar interfaz de línea de comandos interactiva
    CLI();
    
    return 0;
}

// Función para imprimir mensajes de error con formato de color
void print_error(const char *message) {
    // Imprime mensajes de error en color rojo
    fprintf(stderr, RED "ERROR: %s\n" RESET, message);
}

// Inicializar estructura de un DataFrame
void crearDataframe(DataFrame *df, int columnas, int rows, const char *nombre) {
    // Reservar memoria para columnas
    df->columnas = malloc(columnas * sizeof(Columna));
    if (!df->columnas) {
        print_error("Fallo en asignación de memoria para columnas");
        return;
    }
    
    // Establecer metadatos del DataFrame
    df->numColumnas = columnas;
    df->numFilas = rows;
    strncpy(df->indice, nombre, sizeof(df->indice) - 1);

    // Inicializar cada columna con memoria para datos
    for (int i = 0; i < columnas; i++) {
        df->columnas[i].datos = malloc(rows * sizeof(void*));
        df->columnas[i].esNulo = malloc(rows * sizeof(int));
        df->columnas[i].numFilas = rows;
        memset(df->columnas[i].esNulo, 0, rows * sizeof(int));
    }
}

// Liberar memoria de un DataFrame
void liberarMemoriaDF(DataFrame *df) {
    if (df) {
        // Liberar memoria de cada columna
        for (int i = 0; i < df->numColumnas; i++) {
            // Liberar memoria de cada elemento de datos
            for (int j = 0; j < df->numFilas; j++) {
                free(df->columnas[i].datos[j]);
            }
            // Liberar arreglos de datos y nulidad
            free(df->columnas[i].datos);
            free(df->columnas[i].esNulo);
        }
        // Liberar estructura de columnas y DataFrame
        free(df->columnas);
        free(df);
    }
}

// Función para eliminar espacios en blanco de una cadena
void cortarEspacios(char *str) {
    char *start = str;
    char *end = str + strlen(str) - 1;

    // Eliminar espacios al inicio
    while (isspace(*start)) start++;
    
    // Eliminar espacios al final
    while (end > start && isspace(*end)) end--;

    // Reajustar cadena
    *(end + 1) = '\0';
    memmove(str, start, end - start + 2);
}

// Contar número de columnas en una línea CSV
int contarColumnas(const char *line) {
    int count = 1;
    for (int i = 0; line[i]; i++) {
        if (line[i] == ',') count++;
    }
    return count;
}

// Manejar valores nulos en CSV
void verificarNulos(char *linea) {
    char resultado[MAX_LINE_LENGTH * 2] = {0};
    int j = 0;
    int length = strlen(linea);
    int expecting_value = 1;  // Comenzar esperando un valor

    for (int i = 0; i <= length; i++) {
        // Si encuentra separador o fin de línea, procesar campo
        if (i == length || linea[i] == ',' || linea[i] == '\r') {
            if (expecting_value) {
                // Insertar 'NULL' para campos vacíos
                resultado[j++] = 'N';
                resultado[j++] = 'U';
                resultado[j++] = 'L';
                resultado[j++] = 'L';
            }
            
            // Copiar separadores si no es el último carácter
            if (i < length) {
                resultado[j++] = linea[i];
            }

            // Después de un separador, esperar un valor para el siguiente campo
            expecting_value = 1;

            // Manejar saltos de línea
            if (linea[i] == '\n' || linea[i] == '\r') {
                resultado[j++] = '\r';
                resultado[j++] = '\n';
            }
        } else {
            resultado[j++] = linea[i];
            expecting_value = 0;  // Se encontró un valor no vacío
        }
    }

    resultado[j] = '\0';  // Terminar cadena resultante
    strcpy(linea, resultado); // Sobrescribir línea original
}

// Validar formato de fecha YYYY-MM-DD
int fechaValida(const char *date_str) {
    struct tm tm;
    memset(&tm, 0, sizeof(struct tm));

    // Parsear fecha compatible con diferentes sistemas
    if (sscanf(date_str, "%4d-%2d-%2d", &tm.tm_year, &tm.tm_mon, &tm.tm_mday) != 3) {
        return 0;
    }

    tm.tm_year -= 1900;  // Ajustar año desde 1900
    tm.tm_mon -= 1;      // Ajustar mes (0-11)
    time_t t = mktime(&tm);
    return t != -1;
}

// Detectar tipos de columnas: Prioridad Fecha > Numérico > Texto
void tiposColumnas(DataFrame *df) {
    for (int col = 0; col < df->numColumnas; col++) {
        df->columnas[col].tipo = TEXTO;  // Tipo por defecto
        int is_numeric = 1;
        int is_date = 1;

        for (int row = 0; row < df->numFilas; row++) {
            char *value = (char*)df->columnas[col].datos[row];
            if (value == NULL) continue;

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

// Cargar archivo CSV
void cargarCSV(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        print_error("No se puede abrir el archivo");
        return;
    }

    char line[MAX_LINE_LENGTH];
    int numFilas = 0, numColumnas = 0;

    // Primera pasada: Contar filas y columnas
    while (fgets(line, sizeof(line), file)) {
        verificarNulos(line);
        numFilas++;
        if (numFilas == 1) {
            numColumnas = contarColumnas(line);
        }
    }
    rewind(file);

    // Liberar DataFrame existente
    if (active_dataframe) {
        liberarMemoriaDF(active_dataframe);
    }

    // Crear nuevo DataFrame
    active_dataframe = malloc(sizeof(DataFrame));
    if (!active_dataframe) {
        print_error("Fallo en asignación de memoria para DataFrame");
        fclose(file);
        return;
    }
    crearDataframe(active_dataframe, numColumnas, numFilas, "df0");

    // Leer encabezados
    fgets(line, sizeof(line), file);
    char *token = strtok(line, ",");
    for (int col = 0; token != NULL; col++) {
        cortarEspacios(token);
        strncpy(active_dataframe->columnas[col].nombre, token, sizeof(active_dataframe->columnas[col].nombre) - 1);
        token = strtok(NULL, ",");
    }

    // Leer filas de datos
    int current_row = 0;
    while (fgets(line, sizeof(line), file) && current_row < numFilas - 1) {
        verificarNulos(line);
        token = strtok(line, ",");
        for (int col = 0; token != NULL; col++) {
            cortarEspacios(token);

            // Manejo especial de valores nulos
            if (strcmp(token, "NULL") == 0) {
                active_dataframe->columnas[col].datos[current_row] = NULL;
                active_dataframe->columnas[col].esNulo[current_row] = 1;
            } else {
                active_dataframe->columnas[col].datos[current_row] = strdup(token);
                active_dataframe->columnas[col].esNulo[current_row] = 0;
            }
            token = strtok(NULL, ",");
        }
        current_row++;
    }

    // Detectar tipos de columnas
    tiposColumnas(active_dataframe);

    // Actualizar prompt
    snprintf(current_prompt, sizeof(current_prompt), "[%s: %d,%d]:> ", 
             active_dataframe->indice, active_dataframe->numFilas, active_dataframe->numColumnas);

    printf(GREEN "Cargado exitosamente %s con %d filas y %d columnas\n" RESET, 
           filename, active_dataframe->numFilas, active_dataframe->numColumnas);

    showCLI();

    fclose(file);
}

// Mostrar DataFrame completo
void showCLI() {
    if (!active_dataframe) {
        print_error("No hay DataFrame cargado.");
        return;
    }

    printf(WHITE"DataFrame: %s\n" RESET, active_dataframe->indice);
    
    // Imprimir encabezados
    for (int col = 0; col < active_dataframe->numColumnas; col++) {
        printf("%-20s", active_dataframe->columnas[col].nombre);
    }
    printf("\n");

    // Imprimir datos
    for (int row = 0; row < active_dataframe->numFilas; row++) {
        for (int col = 0; col < active_dataframe->numColumnas; col++) {
            char *value = (char*)active_dataframe->columnas[col].datos[row];
            if (value == NULL) {
                printf("NULL                 ");
            } else {
                printf("%-20s", value);
            }
        }
        printf("\n");
    }
}

// Mostrar metadatos del DataFrame
void metaCLI() {
    if (!active_dataframe) {
        print_error("No hay DataFrame cargado.");
        return;
    }

    // Recorrer columnas e imprimir metadatos
    for (int col = 0; col < active_dataframe->numColumnas; col++) {
        int null_count = 0;
        for (int row = 0; row < active_dataframe->numFilas; row++) {
            if (active_dataframe->columnas[col].esNulo[row]) {
                null_count++;
            }
        }

        // Determinar tipo de columna
        char *type_str;
        switch(active_dataframe->columnas[col].tipo) {
            case TEXTO: type_str = "Texto"; break;
            case NUMERICO: type_str = "Numérico"; break;
            case FECHA: type_str = "Fecha"; break;
            default: type_str = "Desconocido"; break;
        }

        // Imprimir metadatos de columna
        printf(GREEN "%s: %s (Valores nulos: %d)\n" RESET, 
               active_dataframe->columnas[col].nombre, 
               type_str, 
               null_count);
    }
}

// Función para ver primeras N filas del DataFrame
void viewCLI(int n) {
    if (!active_dataframe) {
        print_error("No hay DataFrame cargado.");
        return;
    }

    // Mostrar encabezados
    for (int j = 0; j < active_dataframe->numColumnas; j++) {
        printf("%s\t", active_dataframe->columnas[j].nombre);
    }
    printf("\n");

    // Determinar número de filas a mostrar
    int rows_to_show = n < active_dataframe->numFilas ? n : active_dataframe->numFilas;

    // Mostrar filas de datos
    for (int i = 0; i < rows_to_show; i++) {
        for (int j = 0; j < active_dataframe->numColumnas; j++) {
            if (active_dataframe->columnas[j].esNulo[i]) {
                printf("NULL\t");
            } else {
                printf("%s\t", (char *)active_dataframe->columnas[j].datos[i]);
            }
        }
        printf("\n");
    }
}

// Función de comparación de valores según tipo de datos
int compararValores(void *a, void *b, TipoDato tipo, int is_descending) {
    // Manejar casos de valores nulos
    if (a == NULL && b == NULL) return 0;
    if (a == NULL) return is_descending ? 1 : -1;
    if (b == NULL) return is_descending ? -1 : 1;

    char *str_a = (char*)a;
    char *str_b = (char*)b;

    switch(tipo) {
        // Comparación de valores numéricos
        case NUMERICO: {
            double num_a = atof(str_a);
            double num_b = atof(str_b);
            return is_descending ? 
                (num_a < num_b ? 1 : (num_a > num_b ? -1 : 0)) : 
                (num_a < num_b ? -1 : (num_a > num_b ? 1 : 0));
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
            
            return is_descending ? 
                (time_a < time_b ? 1 : (time_a > time_b ? -1 : 0)) :
                (time_a < time_b ? -1 : (time_a > time_b ? 1 : 0));
        }
        // Comparación de cadenas de texto
        case TEXTO: {
            return is_descending ? strcmp(str_b, str_a) : strcmp(str_a, str_b);
        }
        default:
            return 0;
    }
}

// Función para ordenar DataFrame
void sortCLI(const char *column_name, int is_descending) {
    if (!active_dataframe) {
        print_error("No hay DataFrame cargado.");
        return;
    }

    // Buscar índice de la columna
    int column_index = -1;
    for (int i = 0; i < active_dataframe->numColumnas; i++) {
        if (strcmp(active_dataframe->columnas[i].nombre, column_name) == 0) {
            column_index = i;
            break;
        }
    }

    if (column_index == -1) {
        print_error("Columna no encontrada.");
        return;
    }

    // Obtener tipo de datos de la columna
    TipoDato column_type = active_dataframe->columnas[column_index].tipo;

    // Algoritmo de ordenamiento de burbuja
    for (int i = 0; i < active_dataframe->numFilas - 1; i++) {
        for (int j = 0; j < active_dataframe->numFilas - i - 1; j++) {
            void *val1 = active_dataframe->columnas[column_index].datos[j];
            void *val2 = active_dataframe->columnas[column_index].datos[j+1];

            if (compararValores(val1, val2, column_type, is_descending) > 0) {
                // Intercambiar datos de todas las columnas
                for (int col = 0; col < active_dataframe->numColumnas; col++) {
                    void *temp_data = active_dataframe->columnas[col].datos[j];
                    active_dataframe->columnas[col].datos[j] = active_dataframe->columnas[col].datos[j+1];
                    active_dataframe->columnas[col].datos[j+1] = temp_data;

                    // Intercambiar valores de nulidad
                    int temp_null = active_dataframe->columnas[col].esNulo[j];
                    active_dataframe->columnas[col].esNulo[j] = active_dataframe->columnas[col].esNulo[j+1];
                    active_dataframe->columnas[col].esNulo[j+1] = temp_null;
                }
            }
        }
    }

    printf(GREEN "DataFrame ordenado por columna '%s' en orden %s.\n" RESET, 
           column_name, is_descending ? "descendente" : "ascendente");
}

// Interfaz de línea de comandos interactiva
void CLI() {
    char input[MAX_LINE_LENGTH];
    while (1) {
        printf("%s", current_prompt);
        fgets(input, sizeof(input), stdin);
        cortarEspacios(input);

        // Comandos disponibles
        if (strcmp(input, "quit") == 0) {
            break;
        } else if (strncmp(input, "load ", 5) == 0) {
            cargarCSV(input + 5);
        } else if (strcmp(input, "meta") == 0) {
            metaCLI();
        } else if (strncmp(input, "view", 4) == 0) {
            int n = 10; // Mostrar 10 filas por defecto
            
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
        } else if (strncmp(input, "sort ", 5) == 0) {
            char column_name[50];
            char order[10] = "asc";  // Orden ascendente por defecto
            int is_descending = 0;

            // Parsear comando de ordenamiento
            int parsed = sscanf(input + 5, "%49s %9s", column_name, order);
            
            if (parsed == 0) {
                print_error("Comando de ordenamiento inválido");
                continue;
            }

            // Verificar orden
            if (parsed == 2) {
                if (strcmp(order, "asc") == 0) {
                    is_descending = 0;
                } else if (strcmp(order, "des") == 0) {
                    is_descending = 1;
                } else {
                    print_error("Orden de clasificación inválido. Use 'asc' o 'des'.");
                    continue;
                }
            }

            sortCLI(column_name, is_descending);
        } else {
            printf("Comando desconocido: %s\n", input);
        }
    }
}