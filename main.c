#include "lib.h"  // Incluir las funciones definidas en lib.h

int main() { 

  // Mostrar datos del alumno
  printf(GREEN "Datos del alumno:\n" RESET);
  printf("Nombre: Ricardo\n");
  printf("Apellidos: Perez\n");
  printf("Correo: ricardo.perez04@goumh.umh.es\n\n");

  inicializarLista(); // Inicializar la lista al inicio
  printf(GREEN "Sistema de Gestión de Dataframes\n" RESET);

  // Iniciar interfaz de línea de comandos interactiva
  CLI();
  liberarListaCompleta(&listaDF);
  return 0;
}
