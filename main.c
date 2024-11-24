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
DataFrame *dataframe_activo = NULL;  // DataFrame activo actualmente
char promptTerminal[MAX_LINE_LENGTH] = "[?]:> ";  // Prompt de la interfaz interactiva

// Prototipos de todas las funciones utilizadas
void crearDataframe(DataFrame *df, int columnas, int rows, const char *nombre);
void liberarMemoriaDF(DataFrame *df);
int contarColumnas(const char *line);
void cortarEspacios(char *str);
void cargarCSV(const char *filename);
void CLI();
void print_error(const char *message);
void tiposColumnas(DataFrame *df);
void verificarNulos(char *line, int fila);
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

void verificarNulos(char *linea, int fila) {
    // Array para almacenar el resultado procesado.
    char resultado[MAX_LINE_LENGTH * 2] = {0};  
    int j = 0;  // Índice para recorrer el array de resultado
    int length = strlen(linea);  // Longitud de la línea original
    int expecting_value = 1;  // Comienza esperando un valor (no se ha encontrado uno aún)
    
    int colIndex = 0;  // Índice de la columna actual (asumimos que empezamos en la columna 0)

    // Recorre cada carácter de la línea (incluyendo el final de la cadena '\0' para asegurarse de procesar el último campo)
    for (int i = 0; i <= length; i++) {
        // Si encuentra un separador (coma, salto de línea o fin de línea), procesa el campo
        if (i == length || linea[i] == ',' || linea[i] == '\r') {
            // Si estamos esperando un valor (es decir, encontramos un campo vacío)
            if (expecting_value) {
                // Insertamos '1' en el resultado para indicar que el campo es nulo (vacío)
                resultado[j++] = '1';

                // Marcar como nulo en la columna y fila correspondiente
                dataframe_activo->columnas[colIndex].esNulo[fila] = 1;  // Marcar como nulo
                // No incrementamos el contador de nulos aquí, ya que eso se hace en cargarCSV
            }

            // Si no es el último carácter, copia el separador (coma o retorno de carro) al resultado
            if (i < length) {
                resultado[j++] = linea[i];
            }

            // Después de procesar un separador, el siguiente valor debe ser un campo (por lo que esperamos un valor)
            expecting_value = 1;

            // Si encuentra un salto de línea, agrega un retorno de carro y nueva línea al resultado
            if (linea[i] == '\n' || linea[i] == '\r') {
                resultado[j++] = '\r';
                resultado[j++] = '\n';
            }

            // Incrementar el índice de la columna después de procesar un campo
            colIndex++;
        } else {
            // Si el carácter no es un separador, se agrega al resultado
            resultado[j++] = linea[i];
            // Ahora no estamos esperando un valor vacío, ya que encontramos uno no vacío
            expecting_value = 0;
        }
    }

    // Asegurarse de que la cadena resultante esté correctamente terminada con '\0'
    resultado[j] = '\0';

    // Copiar el contenido del array resultado de vuelta a la línea original
    strcpy(linea, resultado);
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
        numFilas++;
        if (numFilas == 1) {
            numColumnas = contarColumnas(line);  // Obtener número de columnas (si es necesario)
        }
    }
    rewind(file);  // Regresar al principio del archivo para la segunda pasada

    // Liberar DataFrame existente
    if (dataframe_activo) {
        liberarMemoriaDF(dataframe_activo);
    }

    // Crear nuevo DataFrame
    dataframe_activo = malloc(sizeof(DataFrame));
    if (!dataframe_activo) {
        print_error("Fallo en asignación de memoria para DataFrame");
        fclose(file);
        return;
    }
    crearDataframe(dataframe_activo, numColumnas, numFilas, "df0");

    // Leer encabezados
    fgets(line, sizeof(line), file);
    char *token = strtok(line, ",");
    for (int col = 0; token != NULL; col++) {
        cortarEspacios(token);
        strncpy(dataframe_activo->columnas[col].nombre, token, sizeof(dataframe_activo->columnas[col].nombre) - 1);
        token = strtok(NULL, ",");
    }

    // Leer filas de datos
    int fila_actual = 0;
    while (fgets(line, sizeof(line), file) && fila_actual < numFilas) {
        // Llamada a verificarNulos para marcar los valores nulos antes de procesar cada fila
        verificarNulos(line, fila_actual);

        token = strtok(line, ",");
        for (int col = 0; token != NULL; col++) {
            cortarEspacios(token);

            // Ya se marcaron los nulos en verificarNulos, entonces solo asignamos los valores
            if (!dataframe_activo->columnas[col].esNulo[fila_actual]) {
                dataframe_activo->columnas[col].datos[fila_actual] = strdup(token);
            }
            token = strtok(NULL, ",");
        }
        fila_actual++;
    }

    // Detectar tipos de columnas
    tiposColumnas(dataframe_activo);

    // Actualizar prompt
    snprintf(promptTerminal, sizeof(promptTerminal), "[%s: %d,%d]:> ", 
             dataframe_activo->indice, dataframe_activo->numFilas, dataframe_activo->numColumnas);

    printf(GREEN "Cargado exitosamente %s con %d filas y %d columnas\n" RESET, 
           filename, dataframe_activo->numFilas, dataframe_activo->numColumnas);

    showCLI();  // Mostrar la interfaz de línea de comandos

    fclose(file);
}



// Función que muestra los metadatos de un DataFrame, incluyendo el nombre de las columnas, su tipo y el número de valores nulos.
void metaCLI() {
    // Verifica si hay un DataFrame cargado, si no, muestra un mensaje de error y termina la función.
    if (!dataframe_activo) {
        print_error("No hay DataFrame cargado.");  // Función que muestra un mensaje de error
        return;  // Sale de la función si no hay un DataFrame activo
    }

    // Recorre todas las columnas del DataFrame para imprimir sus metadatos
    for (int col = 0; col < dataframe_activo->numColumnas; col++) {
        int null_count = 0;  // Contador de valores nulos para la columna actual

        // Recorre todas las filas de la columna actual para contar los valores nulos
        for (int row = 0; row < dataframe_activo->numFilas; row++) {
            // Si el valor de la fila en la columna es nulo (según el array 'esNulo'), incrementa el contador
            if (dataframe_activo->columnas[col].esNulo[row]) {
                null_count++;
            }
        }

        // Determina el tipo de la columna y asigna una descripción al tipo
        char *type_str;  
        switch(dataframe_activo->columnas[col].tipo) {
            case TEXTO: type_str = "Texto"; break;      // Si es de tipo TEXTO, se asigna la descripción "Texto"
            case NUMERICO: type_str = "Numérico"; break; // Si es de tipo NUMERICO, se asigna la descripción "Numérico"
            case FECHA: type_str = "Fecha"; break;      // Si es de tipo FECHA, se asigna la descripción "Fecha"
            default: type_str = "Desconocido"; break;   // Si el tipo es desconocido, se asigna "Desconocido"
        }

        // Imprime los metadatos de la columna: nombre de la columna, tipo y el número de valores nulos
        // Utiliza colores para mejorar la legibilidad (se asume que se usan macros para color como GREEN y RESET)
        printf(GREEN "%s: %s (Valores nulos: %d)\n" RESET, 
               dataframe_activo->columnas[col].nombre,  // Nombre de la columna
               type_str,                               // Tipo de la columna (Texto, Numérico, Fecha, etc.)
               null_count);                             // Número de valores nulos en la columna
    }
}

// Mostrar DataFrame completo
// Mostrar DataFrame completo
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




// Función para ver primeras N filas del DataFrame
void viewCLI(int n) {
    if (!dataframe_activo) {
        print_error("No hay DataFrame cargado.");
        return;
    }

    printf(WHITE"                    DataFrame: %s\n\n" RESET, dataframe_activo->indice);

    // Mostrar encabezados
    for (int j = 0; j < dataframe_activo->numColumnas; j++) {
        printf("%-20s", dataframe_activo->columnas[j].nombre);  // Ajustar la longitud para mejor alineación
    }
    printf("\n");

    // Determinar número de filas a mostrar
    int rows_to_show = (n < dataframe_activo->numFilas) ? n : dataframe_activo->numFilas;

    // Mostrar filas de datos
    for (int i = 0; i < rows_to_show; i++) {
        for (int j = 0; j < dataframe_activo->numColumnas; j++) {
            if (dataframe_activo->columnas[j].esNulo[i]) {
                printf("%-20s", "1");  // Ajustar la longitud para mejor alineación
            } else {
                // Asegurarse de no intentar imprimir punteros NULL
                if (dataframe_activo->columnas[j].datos[i] == NULL) {
                    printf("%-20s", "");  // En caso de que el valor sea NULL, imprimir "NULL"
                } else {
                    printf("%-20s", (char *)dataframe_activo->columnas[j].datos[i]);
                }
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
    if (!dataframe_activo) {
        print_error("No hay DataFrame cargado.");
        return;
    }

    // Buscar índice de la columna
    int column_index = -1;
    for (int i = 0; i < dataframe_activo->numColumnas; i++) {
        if (strcmp(dataframe_activo->columnas[i].nombre, column_name) == 0) {
            column_index = i;
            break;
        }
    }

    if (column_index == -1) {
        print_error("Columna no encontrada.");
        return;
    }

    // Obtener tipo de datos de la columna
    TipoDato column_type = dataframe_activo->columnas[column_index].tipo;

    // Algoritmo de ordenamiento de burbuja
    for (int i = 0; i < dataframe_activo->numFilas - 1; i++) {
        for (int j = 0; j < dataframe_activo->numFilas - i - 1; j++) {
            void *val1 = dataframe_activo->columnas[column_index].datos[j];
            void *val2 = dataframe_activo->columnas[column_index].datos[j+1];

            if (compararValores(val1, val2, column_type, is_descending) > 0) {
                // Intercambiar datos de todas las columnas
                for (int col = 0; col < dataframe_activo->numColumnas; col++) {
                    void *temp_data = dataframe_activo->columnas[col].datos[j];
                    dataframe_activo->columnas[col].datos[j] = dataframe_activo->columnas[col].datos[j+1];
                    dataframe_activo->columnas[col].datos[j+1] = temp_data;

                    // Intercambiar valores de nulidad
                    int temp_null = dataframe_activo->columnas[col].esNulo[j];
                    dataframe_activo->columnas[col].esNulo[j] = dataframe_activo->columnas[col].esNulo[j+1];
                    dataframe_activo->columnas[col].esNulo[j+1] = temp_null;
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
        printf("%s", promptTerminal);
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