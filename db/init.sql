CREATE TABLE IF NOT EXISTS preguntas (
    id INT PRIMARY KEY,
    titulo TEXT NOT NULL,
    mejor_respuesta TEXT NOT NULL,
    respuesta_llm TEXT NOT NULL,
    score FLOAT NOT NULL,
    numero_consultas INT DEFAULT 1
);