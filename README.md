
# enf-analysis

Aufruf von `enf-analysis.py <videoaufnahme>`

Argumente:
- `-sr`: Anzahl der Samples (Fenstergröße) der Methode "repräsentative ENF". Standardwert: 512.
- `-sm`: Anzahl der Samples (Fenstergröße) der Methode "maximale Amplitude". Standardwert: 1024.
- `-ss`: Anzahl der Sekunden die für eine Zeitpunktbestimmung übersprungen werden. Standardwert: 4.
- `-dmd`: Deaktivierung der Bewegungserkennung/Bewegungskompensation.
- `-dp`: Deaktivierung von Grafiken. Es werden keine Grafiken (Spektrogramm, ENF, …) angezeigt.
- `-fvd`: Wenn eine Videodatei analysiert wird, werden die ermittelten Helligkeitswerte gespeichert. Mit dieser Option werden die zwischengespeicherten Werte nicht verwendet. Das Video wird erneut verarbeitet.
- `-lt`: Helligkeits-Schwellwert im Bereich von 0…255. Standardwert: Median.
- `-bo`: Ordnung des Bandpass. Standardwert: 8.
- `-bw`: Breite des Bandpass in Hertz. Bei einer erwarteten Alias-Frequenz von 10 Hz und einer Bandpassbreite von ±0,2 werden Frequenzen von 9,8 - 10,2 Hz gefiltert. Standardwert: ±0,2.
- `-nf`: Frequenz des Stromnetzes in Hz. Standardwert 50.
- `-gt`: Referenz-ENF als csv-Datei. Wenn keine csv-Datei angegeben wird, wird keine Zeitpunktbestimmung durchgeführt. Erwartetes Format:
  - ```
    time,Hz  
    2022-10-07 00:00:00,50.002572
    2022-10-07 00:00:01,50.00089  
    2022-10-07 00:00:02,49.9983  
    2022-10-07 00:00:03,49.995415  
    ... ``