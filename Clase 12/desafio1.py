# Convierte de Programación estructurada a POO
# Transforma este código en un conjunto de clases 
# (Triangulo y Rectángulo) que tengan métodos para calcular su área.

def calcular_area_triangulo(base, altura):
    return (base * altura) / 2

def calcular_area_rectangulo(base, altura):
    return base * altura

base_triangulo = 5
altura_triangulo = 8
area_triangulo = calcular_area_triangulo(base_triangulo, altura_triangulo)
print(f"Área del triángulo: {area_triangulo}")

base_rectangulo = 6
altura_rectangulo = 4
area_rectangulo = calcular_area_rectangulo(base_rectangulo, altura_rectangulo)
print(f"Área del rectángulo: {area_rectangulo}")
