java8:

[info] Benchmark                                        (stringType)  Mode  Cnt     Score    Error  Units
[info] StringDeserializerBenchmark.deserializeDefault          ascii  avgt   50   235.481 ±  0.753  ns/op
[info] StringDeserializerBenchmark.deserializeDefault     ascii-long  avgt   50  2916.533 ± 19.281  ns/op
[info] StringDeserializerBenchmark.deserializeDefault           utf1  avgt   50   795.546 ±  0.999  ns/op
[info] StringDeserializerBenchmark.deserializeDefault           utf2  avgt   50  1653.911 ±  3.445  ns/op
[info] StringDeserializerBenchmark.deserializeDefault           utf3  avgt   50  1543.215 ±  2.783  ns/op
[info] StringDeserializerBenchmark.deserializeDefault          emoji  avgt   50  1239.200 ±  2.953  ns/op
[info] StringDeserializerBenchmark.deserializeDefault         random  avgt   50  7590.291 ± 74.861  ns/op
[info] StringDeserializerBenchmark.deserializeImproved         ascii  avgt   50   110.604 ±  0.291  ns/op
[info] StringDeserializerBenchmark.deserializeImproved    ascii-long  avgt   50   209.662 ±  6.714  ns/op
[info] StringDeserializerBenchmark.deserializeImproved          utf1  avgt   50   312.311 ±  1.044  ns/op
[info] StringDeserializerBenchmark.deserializeImproved          utf2  avgt   50   485.969 ±  1.604  ns/op
[info] StringDeserializerBenchmark.deserializeImproved          utf3  avgt   50   367.750 ±  1.209  ns/op
[info] StringDeserializerBenchmark.deserializeImproved         emoji  avgt   50   467.717 ±  2.615  ns/op
[info] StringDeserializerBenchmark.deserializeImproved        random  avgt   50   982.340 ± 17.383  ns/op
[info] StringDeserializerBenchmark.deserializeJDK              ascii  avgt   50   126.989 ±  0.391  ns/op
[info] StringDeserializerBenchmark.deserializeJDK         ascii-long  avgt   50   230.476 ±  3.561  ns/op
[info] StringDeserializerBenchmark.deserializeJDK               utf1  avgt   50   206.095 ±  1.204  ns/op
[info] StringDeserializerBenchmark.deserializeJDK               utf2  avgt   50   284.506 ±  2.933  ns/op
[info] StringDeserializerBenchmark.deserializeJDK               utf3  avgt   50   385.811 ±  7.876  ns/op
[info] StringDeserializerBenchmark.deserializeJDK              emoji  avgt   50   235.611 ±  1.088  ns/op
[info] StringDeserializerBenchmark.deserializeJDK             random  avgt   50  1031.178 ± 12.184  ns/op
