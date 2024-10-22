#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>

// Tipo enumerado para representar los diferentes tipos de datos en las columnas
typedef enum
{
    TEXTO,
    NUMERICO,
    FECHA
} TipoDato;

// Estructura para representar una columna del dataframe
typedef struct
{
    char nombre[30]; // Nombre de la columna
    TipoDato tipo; // Tipo de datos de la columna (TEXTO, NUMERICO, FECHA)
    void *datos; // Puntero genérico para almacenar los datos de la columna
    unsigned char *esNulo; // Array paralelo, indica valores nulos (1/0: nulo/noNulo)
    int numFilas; // Número de filas en la columna
} Columna;

// Alias para tipos FECHA: 'Fecha' alias de 'struct tm'
typedef struct
{
    int year;
    int month;
    int day;
} Fecha;

// Estructura para representar un dataframe
typedef struct
{
    Columna *columnas;
    int numColumnas; // número de columnas
    int numFilas;
    int *indice;
} Dataframe;

// Función para parsear fecha en formato "AAAA-MM-DD"
int parsearFecha(const char *str, Fecha *fecha)
{
    fecha->year = 0;
    fecha->month = 0;
    fecha->day = 0;

    // Verificar si la cadena no está vacía
    if (str == NULL || strlen(str) == 0)
    {
        return 0; // Error, la fecha es nula o vacía
    }

    // Usamos sscanf para extraer el año, mes y día desde la cadena
    if (sscanf(str, "%d-%d-%d", &fecha->year, &fecha->month, &fecha->day) == 3)
    {
        // Verificar si los valores están en el rango válido
        if (fecha->year >= 1900 && fecha->month >= 1 && fecha->month <= 12 && fecha->day >= 1 && fecha->day <= 31)
        {
            return 1; // Éxito
        }
    }
    return 0; // Fallo en el parseo
}

int main()
{
    // Declaramos la variable 'df' de tipo Dataframe
    Dataframe df;

    df.numColumnas = sizeof(Columna); // Tres columnas: Nombre, Edad, FechaNacimiento
    df.numFilas = 5; // Cinco filas en total (para el ejemplo)
    df.columnas = malloc(df.numColumnas * sizeof(Columna)); // Reservar memoria para columnas

    // Configurar columnas
    strcpy(df.columnas[0].nombre, "Nombre");
    df.columnas[0].tipo = TEXTO;
    df.columnas[0].datos = malloc(df.numFilas * sizeof(char *)); // Reservar memoria para cadenas
    df.columnas[0].esNulo = malloc(df.numFilas * sizeof(unsigned char));

    strcpy(df.columnas[1].nombre, "Edad");
    df.columnas[1].tipo = NUMERICO;
    df.columnas[1].datos = malloc(df.numFilas * sizeof(int)); // Reservar memoria para enteros
    df.columnas[1].esNulo = malloc(df.numFilas * sizeof(unsigned char));

    strcpy(df.columnas[2].nombre, "FechaNacimiento");
    df.columnas[2].tipo = FECHA;
    df.columnas[2].datos = malloc(df.numFilas * sizeof(Fecha)); // Reservar memoria para fechas
    df.columnas[2].esNulo = malloc(df.numFilas * sizeof(unsigned char));

    // Abrir el archivo CSV
    FILE *file = fopen("datos.csv", "r");
    if (!file)
    {
        perror("Error al abrir el archivo");
        return 1;
    }

    char line[1024];
    int row = 0;

    // Saltar la primera línea de encabezados
    fgets(line, 1024, file);

    // Leer las filas de datos
    while (fgets(line, 1024, file) && row < df.numFilas)
    {
        char *token = strtok(line, ",");
        int col = 0;

        // Procesar cada columna
        while (token != NULL)
        {
            if (col == 0)
            { // Columna Nombre (TEXTO)
                if (strlen(token) > 0)
                {
                    ((char **)df.columnas[col].datos)[row] = strdup(token);
                    df.columnas[col].esNulo[row] = 0;
                }
                else
                {
                    df.columnas[col].esNulo[row] = 1; // Es nulo
                }
            }
            else if (col == 1)
            { // Columna Edad (NUMERICO)
                if (strlen(token) > 0)
                {
                    ((int *)df.columnas[col].datos)[row] = atoi(token);
                    df.columnas[col].esNulo[row] = 0;
                }
                else
                {
                    df.columnas[col].esNulo[row] = 1; // Es nulo
                }
            }
            else if (col == 2)
            { // Columna FechaNacimiento (FECHA)
                if (strlen(token) > 0)
                {
                    Fecha fechaNacimiento;
                    if (parsearFecha(token, &fechaNacimiento))
                    {
                        ((Fecha *)df.columnas[col].datos)[row] = fechaNacimiento;
                        df.columnas[col].esNulo[row] = 0;
                    }
                    else
                    {
                        df.columnas[col].esNulo[row] = 1; // Error de parseo, es nulo
                    }
                }
                else
                {
                    df.columnas[col].esNulo[row] = 1; // Es nulo
                }
            }

            token = strtok(NULL, ",");
            col++;
        }

        row++;
    }

    fclose(file);

    // Mostrar los datos leídos
    for (int i = 0; i < df.numFilas; i++)
    {
        printf("Fila %d:\n", i + 1);
        printf("  Nombre: %s\n", ((char **)df.columnas[0].datos)[i]);
        if (df.columnas[1].esNulo[i])
            printf("  Edad: NULL\n");
        else
            printf("  Edad: %d\n", ((int *)df.columnas[1].datos)[i]);
        if (df.columnas[2].esNulo[i])
            printf("  FechaNacimiento: NULL\n");
        else
        {
            Fecha *fecha = &((Fecha *)df.columnas[2].datos)[i];
            printf("  FechaNacimiento: %d-%02d-%02d\n", fecha->year, fecha->month, fecha->day);
        }
    }

    // Liberar memoria al final
    for (int i = 0; i < df.numFilas; i++)
    {
        free(((char **)df.columnas[0].datos)[i]); // Liberar cada cadena de la columna Nombre
    }
    free(df.columnas[0].datos);
    free(df.columnas[0].esNulo);
    free(df.columnas[1].datos);
    free(df.columnas[1].esNulo);
    free(df.columnas[2].datos);
    free(df.columnas[2].esNulo);
    free(df.columnas);

    return 0;
}
