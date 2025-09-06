from carta import Carta
# --- CLASE PADRE ---
class ColeccionDeCartas:
    """Clase base que representa una colección de cartas."""
    def __init__(self, cartas: list[Carta] = None):
        # Si no se pasan cartas, se inicializa como una lista vacía.
        self.cartas = cartas if cartas is not None else []
    
    def __len__(self) -> int:
        return len(self.cartas)
    
    def __str__(self) -> str:
        # Devuelve una lista de las cartas en la colección.
        if not self.cartas:
            return "No hay cartas."
        # Usamos list comprehension para convertir cada carta a string.
        return ", ".join([str(carta) for carta in self.cartas])

    def agregar_carta(self, carta: Carta):
        self.cartas.append(carta)