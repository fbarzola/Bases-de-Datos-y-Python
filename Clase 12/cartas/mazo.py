import random
from carta import Carta

# --- CLASE MAZO ---
# Representa el conjunto de las 40 cartas.
class Mazo:
    """
    Representa un mazo de 40 cartas españolas.
    """
    PALOS = ["Oros", "Copas", "Espadas", "Bastos"]
    NUMEROS = [1, 2, 3, 4, 5, 6, 7, 10, 11, 12] # Baraja de 40 cartas

    def __init__(self):
        # Al crear un mazo, automáticamente creamos las 40 instancias de Carta.
        self.cartas = [
            Carta(palo, numero)
            for palo in self.PALOS
            for numero in self.NUMEROS
        ]
        """
        # Alternativa usando bucles anidados:
        self.cartas = []
        for palo in self.PALOS:
            for numero in self.NUMEROS:
                self.cartas.append(Carta(palo, numero))
        """
        print("Mazo de 40 cartas creado.")

    def __len__(self) -> int:
        # Permite usar la función len() sobre un objeto Mazo. ej: len(mi_mazo)
        return len(self.cartas)

    def __str__(self) -> str:
        return f"Mazo con {len(self)} cartas."
    
    def barajar(self):
        """Mezcla las cartas del mazo de forma aleatoria."""
        random.shuffle(self.cartas)
        print("El mazo ha sido barajado.")

    def repartir_una(self) -> Carta:
        """Saca una carta de la parte superior del mazo."""
        if len(self.cartas) > 0:
            return self.cartas.pop()
        else:
            print("No quedan más cartas en el mazo.")
            return None
