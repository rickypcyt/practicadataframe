#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>  // Módulo para parseo de fechas

// Constantes de configuración del sistema
#define MAX_LINE_LENGTH 4096   // Longitud máxima permitida para una línea
#define MAX_COLUMNS 1000         // Número máximo de columnas en un df
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

// Estructura de Columna: Representa una columna en el df
typedef struct {
    char nombre[50];          // Nombre de la columna
    TipoDato tipo;          // Tipo de datos de la columna
    char **datos;            // Arreglo de punteros a datos
    unsigned int *esNulo;           // Indicador de valores nulos
    int numFilas;          // Número de filas en la columna
} Columna;

// Estructura de df: Contenedor principal de datos
typedef struct {
    Columna *columnas;        // Arreglo de columnas
    int numColumnas;       // Número total de columnas
    int numFilas;          // Número total de filas
    char indice[20];          // Nombre del df
} Dataframe;

// Alias para tipos FECHA: 'Fecha' alias de 'struct tm' (#include <time.h>)
typedef struct tm Fecha;

// Estructura para representar un nodo de la lista
typedef struct NodoLista{
    Dataframe *df; // puntero a Dataframe
    struct NodoLista *siguiente; // Puntero al siguiente nodo de la lista
} Nodo;

// Estructura para representar la lista de Dataframe’s
typedef struct {
    int numDFs; // Número de Dataframes almacenados en la lista
    Nodo *primero; // Puntero al primer Nodo de la lista
} Lista;

// Add this with other global variables
Lista listaDF;  // Global variable to store all dataframes
Dataframe *currentDF = NULL;  // Variable global que apunta al dataframe activo

// Variables globales para gestión del sistema
char promptTerminal[MAX_LINE_LENGTH] = "[?]:> ";  // Prompt de la interfaz interactiva

// Prototipos de todas las funciones utilizadas
void crearDataframe(Dataframe *currentDF, int columnas, int rows, const char *nombre);
void liberarMemoriaDF(Dataframe *currentDF);
int contarColumnas(const char *line);
void cortarEspacios(char *str);
void cargarCSV(const char *filename);
void CLI();
void print_error(const char *message);
void tiposColumnas(Dataframe *df);
void verificarNulos(char *line, int fila, Dataframe *df);
int fechaValida(const char *date_str);
int compararValores(void *a, void *b, TipoDato tipo, int is_descending);
void metaCLI();
void viewCLI(int n);
void sortCLI(const char *column_name, int is_descending);
void saveCLI(const char *filename);
void cambiarDF();

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
        } else if (strcmp(input, "save") == 0) {
            saveCLI(input + 5);
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
                    print_error("Orden de clasificación inválido. Usar 'asc' o 'des'.");
                    continue;
                }
            }

            sortCLI(column_name, is_descending);
        } else if (strncmp(input, "df", 5) == 0){
            int n = 0;
        }
        
        else {
            printf("Comando desconocido: %s\n", input);
        }
    }
}

void cambiarDF() {
    printf("Hello World\n");
}

// Eliminamos la función cortarEspacios ya que no la necesitaremos
void cargarCSV(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        print_error("No se puede abrir el archivo");
        return;
    }

    // Primera pasada: Contar filas y columnas de manera segura
    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    int numFilas = 0, numColumnas = 0;

    // Usar getline para manejar líneas de cualquier longitud
    while ((read = getline(&line, &len, file)) != -1) {
        numFilas++;
        if (numFilas == 1) {
            numColumnas = contarColumnas(line);
            if (numColumnas > MAX_COLUMNS) {
                print_error("Número de columnas excede el límite máximo");
                free(line);
                fclose(file);
                return;
            }
        }
    }

    // Verificar si se encontraron datos
    if (numFilas == 0 || numColumnas == 0) {
        print_error("Archivo vacío o inválido");
        free(line);
        fclose(file);
        return;
    }

    // Reiniciar archivo y buffer
    rewind(file);
    free(line);
    line = NULL;
    len = 0;

    char nombre_df[10];
    snprintf(nombre_df, sizeof(nombre_df), "df%d", listaDF.numDFs);
    listaDF.numDFs++;

    // Liberar df existente si hay uno
    if (currentDF) {
        liberarMemoriaDF(currentDF);
        currentDF = NULL;
    }

    // Crear nuevo df
    currentDF = malloc(sizeof(Dataframe));
    if (!currentDF) {
        print_error("Fallo en asignación de memoria para df");
        free(line);
        fclose(file);
        return;
    }

    crearDataframe(currentDF, numColumnas, numFilas - 1, nombre_df);

    // Leer encabezados
    if ((read = getline(&line, &len, file)) != -1) {
        char *token = strtok(line, ",\n\r");
        int col = 0;
        while (token && col < numColumnas) {
            strncpy(currentDF->columnas[col].nombre, token, sizeof(currentDF->columnas[col].nombre) - 1);
            currentDF->columnas[col].nombre[sizeof(currentDF->columnas[col].nombre) - 1] = '\0';
            token = strtok(NULL, ",\n\r");
            col++;
        }
    }

    // Leer filas de datos
    int fila_actual = 0;
    while ((read = getline(&line, &len, file)) != -1 && fila_actual < numFilas - 1) {
        verificarNulos(line, fila_actual,currentDF);

        char *token = strtok(line, ",\n\r");
        int col = 0;
        while (token && col < numColumnas) {
            if (!currentDF->columnas[col].esNulo[fila_actual]) {
                currentDF->columnas[col].datos[fila_actual] = strdup(token);
            }
            token = strtok(NULL, ",\n\r");
            col++;
        }
        fila_actual++;
    }

    // Liberar recursos
    free(line);
    fclose(file);

    // Detectar tipos y actualizar prompt
    tiposColumnas(currentDF);
    snprintf(promptTerminal, sizeof(promptTerminal), "[%s: %d,%d]:> ", 
             currentDF->indice, currentDF->numFilas, currentDF->numColumnas);

    printf(GREEN "Cargado exitosamente %s con %d filas y %d columnas\n" RESET, 
           filename, currentDF->numFilas, currentDF->numColumnas);
}

void verificarNulos(char *linea, int fila, Dataframe *df) {
    // Eliminar espacios al final de la línea
    cortarEspacios(linea);  // Llama a la función para eliminar los espacios innecesarios

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
                currentDF->columnas[colIndex].esNulo[fila] = 1;  // Marcar como nulo
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

// Función para eliminar espacios en blanco de una cadena
void cortarEspacios(char *str) {
    size_t len = strlen(str);
    if (len > 0 && str[len-1] == '\n') {
        str[len-1] = '\0';  // Elimina el salto de línea al final
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


// Contar número de columnas en una línea CSV
int contarColumnas(const char *line) {
    int count = 1;
    for (int i = 0; line[i]; i++) {
        if (line[i] == ',') count++;
    }
    return count;
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
void tiposColumnas(Dataframe *df) {
    for (int col = 0; col < df->numColumnas; col++) {
        currentDF->columnas[col].tipo = TEXTO;  // Tipo por defecto
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

// Función para imprimir mensajes de error con formato de color
void print_error(const char *message) {
    // Imprime mensajes de error en color rojo
    fprintf(stderr, RED "ERROR: %s\n" RESET, message);
}


// Función modificada para manejar la memoria de manera más segura
void crearDataframe(Dataframe *currentDF, int columnas, int rows, const char *nombre) {
    if (!currentDF || columnas <= 0 || rows <= 0 || !nombre) {
        print_error("Parámetros inválidos en crearDataframe");
        return;
    }

    // Verificar límites máximos
    if (columnas > MAX_COLUMNS) {
        print_error("Número de columnas excede el límite máximo");
        return;
    }

    // Reservar memoria para columnas con verificación
    currentDF->columnas = calloc(columnas, sizeof(Columna));
    if (!currentDF->columnas) {
        print_error("Fallo en asignación de memoria para columnas");
        return;
    }
    
    currentDF->numColumnas = columnas;
    currentDF->numFilas = rows;
    strncpy(currentDF->indice, nombre, sizeof(currentDF->indice) - 1);
    currentDF->indice[sizeof(currentDF->indice) - 1] = '\0';

    // Inicializar cada columna
    for (int i = 0; i < columnas; i++) {
        // Usar calloc en lugar de malloc para inicializar a 0
        currentDF->columnas[i].datos = calloc(rows, sizeof(void*));
        currentDF->columnas[i].esNulo = calloc(rows, sizeof(int));
        
        if (!currentDF->columnas[i].datos || !currentDF->columnas[i].esNulo) {
            // Si falla la asignación, liberar memoria previamente asignada
            for (int j = 0; j < i; j++) {
                free(currentDF->columnas[j].datos);
                free(currentDF->columnas[j].esNulo);
            }
            free(currentDF->columnas);
            print_error("Fallo en asignación de memoria para datos de columna");
            return;
        }
        
        currentDF->columnas[i].numFilas = rows;
    }
}

// Liberar memoria de un currentDF
void liberarMemoriaDF(Dataframe *df) {
    if (currentDF) {
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
        // Liberar estructura de columnas y currentDF
        free(df->columnas);
        free(df);
    }
}

// Función principal: Punto de entrada del programa
int main() {
    // Mostrar mensaje de bienvenida con color verde
    printf(GREEN "Sistema de Gestión de Dataframes\n" RESET);
    
    // Iniciar interfaz de línea de comandos interactiva
    CLI();
    
    return 0;
}

// Función que muestra los metadatos de un currentDF, incluyendo el nombre de las columnas, su tipo y el número de valores nulos.
void metaCLI() {
    // Verifica si hay un currentDF cargado, si no, muestra un mensaje de error y termina la función.
    if (!currentDF) {
        print_error("No hay currentDF cargado.");  // Función que muestra un mensaje de error
        return;  // Sale de la función si no hay un currentDF activo
    }

    // Recorre todas las columnas del currentDF para imprimir sus metadatos
    for (int col = 0; col < currentDF->numColumnas; col++) {
        int null_count = 0;  // Contador de valores nulos para la columna actual

        // Recorre todas las filas de la columna actual para contar los valores nulos
        for (int row = 0; row < currentDF->numFilas; row++) {
            // Si el valor de la fila en la columna es nulo (según el array 'esNulo'), incrementa el contador
            if (currentDF->columnas[col].esNulo[row]) {
                null_count++;
            }
        }

        // Determina el tipo de la columna y asigna una descripción al tipo
        char *type_str;  
        switch(currentDF->columnas[col].tipo) {
            case TEXTO: type_str = "Texto"; break;      // Si es de tipo TEXTO, se asigna la descripción "Texto"
            case NUMERICO: type_str = "Numérico"; break; // Si es de tipo NUMERICO, se asigna la descripción "Numérico"
            case FECHA: type_str = "Fecha"; break;      // Si es de tipo FECHA, se asigna la descripción "Fecha"
            default: type_str = "Desconocido"; break;   // Si el tipo es desconocido, se asigna "Desconocido"
        }

        // Imprime los metadatos de la columna: nombre de la columna, tipo y el número de valores nulos
        // Utiliza colores para mejorar la legibilidad (se asume que se usan macros para color como GREEN y RESET)
        printf(GREEN "%s: %s (Valores nulos: %d)\n" RESET, 
               currentDF->columnas[col].nombre,  // Nombre de la columna
               type_str,                               // Tipo de la columna (Texto, Numérico, Fecha, etc.)
               null_count);                             // Número de valores nulos en la columna
    }
}

// Mostrar currentDF completo

// Función para ver primeras N filas del currentDF
void viewCLI(int n) {
    if (!currentDF) {
        print_error("No hay currentDF cargado.");
        return;
    }

    printf(WHITE"                    df: %s\n\n" RESET, currentDF->indice);

    // Mostrar encabezados
    for (int j = 0; j < currentDF->numColumnas; j++) {
        printf("%s", currentDF->columnas[j].nombre);
        if (j < currentDF->numColumnas - 1) {
            printf(",");
        }
    }
    printf("\n");

    // Determinar número de filas a mostrar
    int rows_to_show = (n < currentDF->numFilas) ? n : currentDF->numFilas;

    // Mostrar filas de datos
    for (int i = 0; i < rows_to_show; i++) {
        for (int j = 0; j < currentDF->numColumnas; j++) {
            if (currentDF->columnas[j].esNulo[i]) {
                printf("1");
            } else {
                if (currentDF->columnas[j].datos[i] == NULL) {
                    printf("");
                } else {
                    printf("%s", (char *)currentDF->columnas[j].datos[i]);
                }
            }
            // Agregar coma si no es la última columna
            if (j < currentDF->numColumnas - 1) {
                printf(",");
            }
        }
        printf("\n");
    }
}
// Función para ordenar currentDF
void sortCLI(const char *column_name, int is_descending) {
    if (!currentDF) {
        print_error("No hay df cargado.");
        return;
    }

    // Buscar índice de la columna
    int column_index = -1;
    for (int i = 0; i < currentDF->numColumnas; i++) {
        if (strcmp(currentDF->columnas[i].nombre, column_name) == 0) {
            column_index = i;
            break;
        }
    }

    if (column_index == -1) {
        print_error("Columna no encontrada.");
        return;
    }

    // Obtener tipo de datos de la columna
    TipoDato column_type = currentDF->columnas[column_index].tipo;

    // Algoritmo de ordenamiento de burbuja
    for (int i = 0; i < currentDF->numFilas - 1; i++) {
        for (int j = 0; j < currentDF->numFilas - i - 1; j++) {
            void *val1 = currentDF->columnas[column_index].datos[j];
            void *val2 = currentDF->columnas[column_index].datos[j+1];

            if (compararValores(val1, val2, column_type, is_descending) > 0) {
                // Intercambiar datos de todas las columnas
                for (int col = 0; col < currentDF->numColumnas; col++) {
                    void *temp_data = currentDF->columnas[col].datos[j];
                    currentDF->columnas[col].datos[j] = currentDF->columnas[col].datos[j+1];
                    currentDF->columnas[col].datos[j+1] = temp_data;

                    // Intercambiar valores de nulidad
                    int temp_null = currentDF->columnas[col].esNulo[j];
                    currentDF->columnas[col].esNulo[j] = currentDF->columnas[col].esNulo[j+1];
                    currentDF->columnas[col].esNulo[j+1] = temp_null;
                }
            }
        }
    }

    printf(GREEN "currentDF ordenado por columna '%s' en orden %s.\n" RESET, 
           column_name, is_descending ? "descendente" : "ascendente");
}

void saveCLI(const char *filename) {
    // Verificar si hay un currentDF activo
    if (!currentDF) {
        print_error("No hay currentDF activo para guardar.");
        return;
    }

    // Verificar si el nombre de archivo termina con .csv
    char cleaned_filename[MAX_FILENAME];
    snprintf(cleaned_filename, sizeof(cleaned_filename), "%s", filename);
    
    // Eliminar espacios en blanco del nombre de archivo
    cortarEspacios(cleaned_filename);

    // Verificar que el nombre de archivo no esté vacío y termine con .csv
    if (strlen(cleaned_filename) == 0) {
        print_error("Nombre de archivo inválido.");
        return;
    }

    // Verificar explícitamente si termina con .csv
    int len = strlen(cleaned_filename);
    if (len < 4 || strcmp(cleaned_filename + len - 4, ".csv") != 0) {
        print_error("El archivo debe tener extensión .csv");
        return;
    }

    // Abrir archivo para escritura
    FILE *file = fopen(cleaned_filename, "w");
    if (!file) {
        char error_msg[MAX_FILENAME + 50];
        snprintf(error_msg, sizeof(error_msg), "No se puede crear el archivo: %s", cleaned_filename);
        print_error(error_msg);
        return;
    }

    // Escribir encabezados (nombres de columnas)
    for (int col = 0; col < currentDF->numColumnas; col++) {
        fprintf(file, "%s", currentDF->columnas[col].nombre);
        if (col < currentDF->numColumnas - 1) {
            fprintf(file, ",");
        }
    }
    fprintf(file, "\n");

    // Escribir datos
    for (int row = 0; row < currentDF->numFilas; row++) {
        for (int col = 0; col < currentDF->numColumnas; col++) {
            // Si el valor es nulo, escribir cadena vacía
            if (currentDF->columnas[col].esNulo[row]) {
                fprintf(file, "");
            } else {
                // Escribir valor de la celda
                char *valor = (char *)currentDF->columnas[col].datos[row];
                fprintf(file, "%s", valor ? valor : "");
            }
            // Añadir coma entre columnas, excepto en la última
            if (col < currentDF->numColumnas - 1) {
                fprintf(file, ",");
            }
        }
        // Nueva línea después de cada fila
        fprintf(file, "\n");
    }
    fclose(file);
    printf(GREEN "df guardado exitosamente en %s\n" RESET, cleaned_filename);
}
