import random
from carta import Carta 
from coleccion import ColeccionDeCartas

# --- CLASE HIJA: MAZO ---
# Mazo hereda de ColeccionDeCartas.
class Mazo(ColeccionDeCartas):
    """Mazo de 40 cartas españolas que hereda de ColeccionDeCartas."""
    PALOS = ["Oros", "Copas", "Espadas", "Bastos"]
    NUMEROS = [1, 2, 3, 4, 5, 6, 7, 10, 11, 12]

    def __init__(self):
        # Llama al constructor de la clase padre (ColeccionDeCartas).
        super().__init__()
        # Ahora añade su propia lógica: crear las 40 cartas.
        self.cartas = [
            Carta(palo, numero)
            for palo in self.PALOS
            for numero in self.NUMEROS
        ]

    def barajar(self):
        random.shuffle(self.cartas)

    def repartir_una(self) -> Carta:
        if len(self) > 0:
            return self.cartas.pop()
        return None