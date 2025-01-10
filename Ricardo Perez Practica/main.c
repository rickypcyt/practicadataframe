#include "lib.h"  // Incluir las funciones definidas en lib.h

int main() {
  inicializarLista(); // Inicializar la lista al inicio
  // Mostrar mensaje de bienvenida con color verde
  printf(GREEN "Sistema de Gestión de Dataframes\n" RESET);

  // Iniciar interfaz de línea de comandos interactiva
  CLI();
  liberarListaCompleta(&listaDF); // Add this line
  return 0;
}
