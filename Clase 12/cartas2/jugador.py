from mano import Mano
from mazo import Mazo
# --- CLASE JUGADOR ---
# Esta clase "usa" otras clases (tiene una Mano). Esto se llama Composición.
class Jugador:
    def __init__(self, nombre: str):
        self.nombre = nombre
        # Cada jugador TIENE UNA mano.
        self.mano = Mano()

    def tomar_carta(self, mazo: Mazo):
        """El jugador toma una carta del mazo y la añade a su mano."""
        carta = mazo.repartir_una()
        if carta:
            self.mano.agregar_carta(carta)
            return carta
        return None

    def __str__(self) -> str:
        return f"Jugador: {self.nombre}\nMano: {self.mano}"