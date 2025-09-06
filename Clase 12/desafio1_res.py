class Triangulo:
    def __init__(self, base, altura):
        self.base = base
        self.altura = altura

    def calcular_area(self):
        return (self.base * self.altura) / 2

class Rectangulo:
    def __init__(self, base, altura):
        self.base = base
        self.altura = altura

    def calcular_area(self):
        return self.base * self.altura

# Crear instancias de Triangulo y Rectangulo
triangulo = Triangulo(5, 8)
area_triangulo = triangulo.calcular_area()
print(f"Área del triángulo: {area_triangulo}")

rectangulo = Rectangulo(6, 4)
area_rectangulo = float(rectangulo.calcular_area())
print(f"Área del rectángulo: {area_rectangulo}")
