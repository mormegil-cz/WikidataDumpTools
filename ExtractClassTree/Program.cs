using ExtractClassTree;

using var processor = new ClassTreeProcessor(@"/media/petr/Bigfoot/Wikidata-Dump/2023-08-18-truthy.nt.bz2", @"/media/petr/Bigfoot/Wikidata-Dump/2023-08-18-class-tree.bin");
processor.Run();