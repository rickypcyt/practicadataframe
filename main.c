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
#define ANSI_COLOR_RED     "\x1b[31m"     // Rojo para mensajes de error
#define ANSI_COLOR_GREEN   "\x1b[32m"     // Verde para mensajes de éxito
#define ANSI_COLOR_WHITE   "\x1b[37m"     // Blanco para texto general
#define ANSI_COLOR_RESET   "\x1b[0m"      // Resetear formato de color

// Enumeración de tipos de datos soportados
typedef enum {
    TYPE_TEXT,      // Tipo cadena de texto
    TYPE_NUMERIC,   // Tipo numérico (punto flotante)
    TYPE_DATE       // Tipo fecha
} DataType;

// Estructura de Columna: Representa una columna en el DataFrame
typedef struct {
    char name[50];          // Nombre de la columna
    DataType type;          // Tipo de datos de la columna
    void **data;            // Arreglo de punteros a datos
    int *is_null;           // Indicador de valores nulos
    int row_count;          // Número de filas en la columna
} Column;

// Estructura de DataFrame: Contenedor principal de datos
typedef struct {
    Column *columns;        // Arreglo de columnas
    int column_count;       // Número total de columnas
    int row_count;          // Número total de filas
    char name[20];          // Nombre del DataFrame
} DataFrame;

// Variables globales para gestión del sistema
DataFrame *active_dataframe = NULL;  // DataFrame activo actualmente
char current_prompt[MAX_LINE_LENGTH] = "[?]:> ";  // Prompt de la interfaz interactiva

// Prototipos de todas las funciones utilizadas
void initialize_dataframe(DataFrame *df, int columns, int rows, const char *name);
void free_dataframe(DataFrame *df);
int count_csv_columns(const char *line);
void trim_whitespace(char *str);
void load_csv(const char *filename);
void show_dataframe();
void print_error(const char *message);
void interactive_cli();
void detect_column_types(DataFrame *df);
void handle_null_values(char *linea);
int is_valid_date(const char *date_str);
void show_metadata();
void view_dataframe(int n);
int compare_values(void *a, void *b, DataType type, int is_descending);
void sort_dataframe(const char *column_name, int is_descending);

// Función principal: Punto de entrada del programa
int main() {
    // Mostrar mensaje de bienvenida con color verde
    printf(ANSI_COLOR_GREEN "Sistema de Gestión de Dataframes\n" ANSI_COLOR_RESET);
    
    // Iniciar interfaz de línea de comandos interactiva
    interactive_cli();
    
    return 0;
}

// Función para imprimir mensajes de error con formato de color
void print_error(const char *message) {
    // Imprime mensajes de error en color rojo
    fprintf(stderr, ANSI_COLOR_RED "ERROR: %s\n" ANSI_COLOR_RESET, message);
}

// Inicializar estructura de un DataFrame
void initialize_dataframe(DataFrame *df, int columns, int rows, const char *name) {
    // Reservar memoria para columnas
    df->columns = malloc(columns * sizeof(Column));
    if (!df->columns) {
        print_error("Fallo en asignación de memoria para columnas");
        return;
    }
    
    // Establecer metadatos del DataFrame
    df->column_count = columns;
    df->row_count = rows;
    strncpy(df->name, name, sizeof(df->name) - 1);

    // Inicializar cada columna con memoria para datos
    for (int i = 0; i < columns; i++) {
        df->columns[i].data = malloc(rows * sizeof(void*));
        df->columns[i].is_null = malloc(rows * sizeof(int));
        df->columns[i].row_count = rows;
        memset(df->columns[i].is_null, 0, rows * sizeof(int));
    }
}

// Liberar memoria de un DataFrame
void free_dataframe(DataFrame *df) {
    if (df) {
        // Liberar memoria de cada columna
        for (int i = 0; i < df->column_count; i++) {
            // Liberar memoria de cada elemento de datos
            for (int j = 0; j < df->row_count; j++) {
                free(df->columns[i].data[j]);
            }
            // Liberar arreglos de datos y nulidad
            free(df->columns[i].data);
            free(df->columns[i].is_null);
        }
        // Liberar estructura de columnas y DataFrame
        free(df->columns);
        free(df);
    }
}

// Función para eliminar espacios en blanco de una cadena
void trim_whitespace(char *str) {
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
int count_csv_columns(const char *line) {
    int count = 1;
    for (int i = 0; line[i]; i++) {
        if (line[i] == ',') count++;
    }
    return count;
}

// Manejar valores nulos en CSV
void handle_null_values(char *linea) {
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
int is_valid_date(const char *date_str) {
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
void detect_column_types(DataFrame *df) {
    for (int col = 0; col < df->column_count; col++) {
        df->columns[col].type = TYPE_TEXT;  // Tipo por defecto
        int is_numeric = 1;
        int is_date = 1;

        for (int row = 0; row < df->row_count; row++) {
            char *value = (char*)df->columns[col].data[row];
            if (value == NULL) continue;

            // Verificar si es numérico
            char *endptr;
            strtod(value, &endptr);
            if (endptr == value) {
                is_numeric = 0;
            }

            // Verificar si es fecha
            if (!is_valid_date(value)) {
                is_date = 0;
            }
        }

        // Establecer tipo con prioridad: Fecha > Numérico > Texto
        if (is_date) {
            df->columns[col].type = TYPE_DATE;
        } else if (is_numeric) {
            df->columns[col].type = TYPE_NUMERIC;
        }
    }
}

// Cargar archivo CSV
void load_csv(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        print_error("No se puede abrir el archivo");
        return;
    }

    char line[MAX_LINE_LENGTH];
    int row_count = 0, column_count = 0;

    // Primera pasada: Contar filas y columnas
    while (fgets(line, sizeof(line), file)) {
        handle_null_values(line);
        row_count++;
        if (row_count == 1) {
            column_count = count_csv_columns(line);
        }
    }
    rewind(file);

    // Liberar DataFrame existente
    if (active_dataframe) {
        free_dataframe(active_dataframe);
    }

    // Crear nuevo DataFrame
    active_dataframe = malloc(sizeof(DataFrame));
    if (!active_dataframe) {
        print_error("Fallo en asignación de memoria para DataFrame");
        fclose(file);
        return;
    }
    initialize_dataframe(active_dataframe, column_count, row_count, "df0");

    // Leer encabezados
    fgets(line, sizeof(line), file);
    char *token = strtok(line, ",");
    for (int col = 0; token != NULL; col++) {
        trim_whitespace(token);
        strncpy(active_dataframe->columns[col].name, token, sizeof(active_dataframe->columns[col].name) - 1);
        token = strtok(NULL, ",");
    }

    // Leer filas de datos
    int current_row = 0;
    while (fgets(line, sizeof(line), file) && current_row < row_count - 1) {
        handle_null_values(line);
        token = strtok(line, ",");
        for (int col = 0; token != NULL; col++) {
            trim_whitespace(token);

            // Manejo especial de valores nulos
            if (strcmp(token, "NULL") == 0) {
                active_dataframe->columns[col].data[current_row] = NULL;
                active_dataframe->columns[col].is_null[current_row] = 1;
            } else {
                active_dataframe->columns[col].data[current_row] = strdup(token);
                active_dataframe->columns[col].is_null[current_row] = 0;
            }
            token = strtok(NULL, ",");
        }
        current_row++;
    }

    // Detectar tipos de columnas
    detect_column_types(active_dataframe);

    // Actualizar prompt
    snprintf(current_prompt, sizeof(current_prompt), "[%s: %d,%d]:> ", 
             active_dataframe->name, active_dataframe->row_count, active_dataframe->column_count);

    printf(ANSI_COLOR_GREEN "Cargado exitosamente %s con %d filas y %d columnas\n" ANSI_COLOR_RESET, 
           filename, active_dataframe->row_count, active_dataframe->column_count);

    fclose(file);
}

// Mostrar DataFrame completo
void show_dataframe() {
    if (!active_dataframe) {
        print_error("No hay DataFrame cargado.");
        return;
    }

    printf(ANSI_COLOR_WHITE "DataFrame: %s\n" ANSI_COLOR_RESET, active_dataframe->name);
    
    // Imprimir encabezados
    for (int col = 0; col < active_dataframe->column_count; col++) {
        printf("%-20s", active_dataframe->columns[col].name);
    }
    printf("\n");

    // Imprimir datos
    for (int row = 0; row < active_dataframe->row_count; row++) {
        for (int col = 0; col < active_dataframe->column_count; col++) {
            char *value = (char*)active_dataframe->columns[col].data[row];
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
void show_metadata() {
    if (!active_dataframe) {
        print_error("No hay DataFrame cargado.");
        return;
    }

    // Recorrer columnas e imprimir metadatos
    for (int col = 0; col < active_dataframe->column_count; col++) {
        int null_count = 0;
        for (int row = 0; row < active_dataframe->row_count; row++) {
            if (active_dataframe->columns[col].is_null[row]) {
                null_count++;
            }
        }

        // Determinar tipo de columna
        char *type_str;
        switch(active_dataframe->columns[col].type) {
            case TYPE_TEXT: type_str = "Texto"; break;
            case TYPE_NUMERIC: type_str = "Numérico"; break;
            case TYPE_DATE: type_str = "Fecha"; break;
            default: type_str = "Desconocido"; break;
        }

        // Imprimir metadatos de columna
        printf(ANSI_COLOR_GREEN "%s: %s (Valores nulos: %d)\n" ANSI_COLOR_RESET, 
               active_dataframe->columns[col].name, 
               type_str, 
               null_count);
    }
}

// Función para ver primeras N filas del DataFrame
void view_dataframe(int n) {
    if (!active_dataframe) {
        print_error("No hay DataFrame cargado.");
        return;
    }

    // Mostrar encabezados
    for (int j = 0; j < active_dataframe->column_count; j++) {
        printf("%s\t", active_dataframe->columns[j].name);
    }
    printf("\n");

    // Determinar número de filas a mostrar
    int rows_to_show = n < active_dataframe->row_count ? n : active_dataframe->row_count;

    // Mostrar filas de datos
    for (int i = 0; i < rows_to_show; i++) {
        for (int j = 0; j < active_dataframe->column_count; j++) {
            if (active_dataframe->columns[j].is_null[i]) {
                printf("NULL\t");
            } else {
                printf("%s\t", (char *)active_dataframe->columns[j].data[i]);
            }
        }
        printf("\n");
    }
}

// Función de comparación de valores según tipo de datos
int compare_values(void *a, void *b, DataType type, int is_descending) {
    // Manejar casos de valores nulos
    if (a == NULL && b == NULL) return 0;
    if (a == NULL) return is_descending ? 1 : -1;
    if (b == NULL) return is_descending ? -1 : 1;

    char *str_a = (char*)a;
    char *str_b = (char*)b;

    switch(type) {
        // Comparación de valores numéricos
        case TYPE_NUMERIC: {
            double num_a = atof(str_a);
            double num_b = atof(str_b);
            return is_descending ? 
                (num_a < num_b ? 1 : (num_a > num_b ? -1 : 0)) : 
                (num_a < num_b ? -1 : (num_a > num_b ? 1 : 0));
        }
        // Comparación de fechas
        case TYPE_DATE: {
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
        case TYPE_TEXT: {
            return is_descending ? strcmp(str_b, str_a) : strcmp(str_a, str_b);
        }
        default:
            return 0;
    }
}

// Función para ordenar DataFrame
void sort_dataframe(const char *column_name, int is_descending) {
    if (!active_dataframe) {
        print_error("No hay DataFrame cargado.");
        return;
    }

    // Buscar índice de la columna
    int column_index = -1;
    for (int i = 0; i < active_dataframe->column_count; i++) {
        if (strcmp(active_dataframe->columns[i].name, column_name) == 0) {
            column_index = i;
            break;
        }
    }

    if (column_index == -1) {
        print_error("Columna no encontrada.");
        return;
    }

    // Obtener tipo de datos de la columna
    DataType column_type = active_dataframe->columns[column_index].type;

    // Algoritmo de ordenamiento de burbuja
    for (int i = 0; i < active_dataframe->row_count - 1; i++) {
        for (int j = 0; j < active_dataframe->row_count - i - 1; j++) {
            void *val1 = active_dataframe->columns[column_index].data[j];
            void *val2 = active_dataframe->columns[column_index].data[j+1];

            if (compare_values(val1, val2, column_type, is_descending) > 0) {
                // Intercambiar datos de todas las columnas
                for (int col = 0; col < active_dataframe->column_count; col++) {
                    void *temp_data = active_dataframe->columns[col].data[j];
                    active_dataframe->columns[col].data[j] = active_dataframe->columns[col].data[j+1];
                    active_dataframe->columns[col].data[j+1] = temp_data;

                    // Intercambiar valores de nulidad
                    int temp_null = active_dataframe->columns[col].is_null[j];
                    active_dataframe->columns[col].is_null[j] = active_dataframe->columns[col].is_null[j+1];
                    active_dataframe->columns[col].is_null[j+1] = temp_null;
                }
            }
        }
    }

    printf(ANSI_COLOR_GREEN "DataFrame ordenado por columna '%s' en orden %s.\n" ANSI_COLOR_RESET, 
           column_name, is_descending ? "descendente" : "ascendente");
}

// Interfaz de línea de comandos interactiva
void interactive_cli() {
    char input[MAX_LINE_LENGTH];
    while (1) {
        printf("%s", current_prompt);
        fgets(input, sizeof(input), stdin);
        trim_whitespace(input);

        // Comandos disponibles
        if (strcmp(input, "quit") == 0) {
            break;
        } else if (strncmp(input, "load ", 5) == 0) {
            load_csv(input + 5);
        } else if (strcmp(input, "show") == 0) {
            show_dataframe();
        } else if (strcmp(input, "meta") == 0) {
            show_metadata();
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
            
            view_dataframe(n);
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

            sort_dataframe(column_name, is_descending);
        } else {
            printf("Comando desconocido: %s\n", input);
        }
    }
}