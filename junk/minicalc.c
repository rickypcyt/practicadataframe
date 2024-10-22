
// sumar numeros multiplicar dividir LISTO
// cli para poner numeros tipo a b c LISTO
// elevar a la potencia, raiz cuadrada
// preguntar al inicio del programa que quiere hacer si sumar restar o que y mostar opciones LISTO
// respuestas con colores

#include <stdio.h>
#include <string.h>

// Colores para la salida en consola
#define BLK "\e[0;30m"
#define RED "\e[0;31m"
#define GRN "\e[0;32m"
#define YEL "\e[0;33m"
#define BLU "\e[0;34m"
#define MAG "\e[0;35m"
#define CYN "\e[0;36m"
#define WHT "\e[0;37m"

// Resetea el color
#define reset "\e[0m"

int main()
{
    float num1, num2;
    char mode[20];

    // Solicita el primer numero
    printf(CYN "\nInserta el primer numero: " reset WHT);
    scanf("%f", &num1);
    printf(YEL "\nPrimer numero: %f \n" reset, num1);

    // Solicita el segundo numero
    printf(CYN "\nInserta el segundo numero: " reset WHT);
    scanf("%f", &num2);
    printf(YEL "\nSegundo numero: %f \n" reset, num2);

    printf(GRN "\nHas insertado: %f, %f \n" reset, num1, num2);

    // Solicita la operacion a realizar
    printf(WHT "\nQue quieres hacer con estos numeros?\n" reset);
    printf(WHT "Puedes sumar, restar, dividir o multiplicar:\n\n" reset MAG);
    scanf(" %19s", mode); // Lee la operacion a realizar

    // Realiza la operacion segun la opcion elegida
    if (strcmp(mode, "sumar") == 0)
    {
        float totalsum = num1 + num2; // Sumar
        printf("\nLa suma de los numeros insertados da: %f \n", totalsum);
    }
    else if (strcmp(mode, "restar") == 0)
    {
        float totalrest = num1 - num2; // Restar
        printf("\nLa resta de los numeros insertados da: %f \n", totalrest);
    }
    else if (strcmp(mode, "dividir") == 0)
    {
        // Verifica que no se divida por cero
        if (num2 != 0)
        {
            float totaldiv = num1 / num2; // Dividir
            printf("\nLa division de los numeros insertados da: %f \n", totaldiv);
        }
        else
        {
            printf(RED "ERROR: Division por cero no permitida.\n" reset);
        }
    }
    else if (strcmp(mode, "multiplicar") == 0)
    {
        float totalmult = num1 * num2; // Multiplicar
        printf("\nLa multiplicacion de los numeros insertados da: %f \n", totalmult);
    }
    else
    {
        printf(RED "ERROR: Opcion no valida.\n" reset); // Manejo de error
    }

    return 0;
}

// For later maybe:

// int arr[2] = {num1,num2};
// for (int i = 0; i < argc; i++) {
//     printf("%s\n", argv[i]);
// }
// argv[1] points to the first command line argument and argv[argc-1] points to the last argument.
// return 0;
