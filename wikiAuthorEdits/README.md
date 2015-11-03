# wikiAuthorEdits

Übung mit Apache Flink und Wikipedia XML Dump Files

Testinputs von: https://dumps.wikimedia.org/backup-index.html

Liest aus Wikipedia XML Dump Files (Format wird dabei noch nicht geprüft!) die Namen 
der Autoren ein und gibt sie mit der Summe ihrer Edits für Pages in Namespace 0 aus. Dabei werden die XML-Files aus dem Input-Ordner eingelesen. Getestet wurde mit kleinen XML-Files.