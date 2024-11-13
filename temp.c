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

// Función para eliminar espacios en blanco al inicio y final de una cadena
void trim(char *str)
{
    char *start = str;
    char *end = str + strlen(str) - 1;

    while (isspace((unsigned char)*start))
        start++;
    while (end > start && isspace((unsigned char)*end))
        end--;

    *(end + 1) = '\0';
    memmove(str, start, end - start + 2);
}

//Contar filas
    int numColumnas = 0;
    int numFilas = 0;

    printf("\nEl archivo tiene %d filas y %d columnas.\n", numFilas, numColumnas);

    