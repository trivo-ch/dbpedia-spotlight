package org.dbpedia.spotlight.db

import concurrent.{SpotterWrapper, TokenizerWrapper}
import org.dbpedia.spotlight.db.memory.{MemoryContextStore, MemoryStore}
import model._
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}
import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import opennlp.tools.postag.{POSModel, POSTaggerME}
import org.dbpedia.spotlight.disambiguate.mixtures.UnweightedMixture
import org.dbpedia.spotlight.db.similarity.{ContextSimilarity, GenerativeContextSimilarity, NoContextSimilarity}

import scala.collection.JavaConverters._
import org.dbpedia.spotlight.model.SpotterConfiguration.SpotterPolicy
import org.dbpedia.spotlight.model.SpotlightConfiguration.DisambiguationPolicy
import org.dbpedia.spotlight.disambiguate.ParagraphDisambiguatorJ
import org.dbpedia.spotlight.spot.{SpotXmlParser, Spotter}
import java.io._
import java.util.{Locale, Properties}

import opennlp.tools.chunker.ChunkerModel
import opennlp.tools.namefind.TokenNameFinderModel
import stem.SnowballStemmer
import tokenize.{LanguageIndependentTokenizer, OpenNLPTokenizer}
import org.dbpedia.spotlight.exceptions.ConfigurationException
import org.dbpedia.spotlight.util.MathUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path


class SpotlightModel(val tokenizer: TextTokenizer,
                     val spotters: java.util.Map[SpotterPolicy, Spotter],
                     val disambiguators: java.util.Map[DisambiguationPolicy, ParagraphDisambiguatorJ],
                     val properties: Properties)

object SpotlightModel {

  def loadStopwords(modelFolder: File): Set[String] = scala.io.Source.fromFile(new File(modelFolder, "stopwords.list")).getLines().map(_.trim()).toSet
  def loadStopwordsFromHadoop(modelFolder: Path, conf: Configuration): Set[String] = {
    val fs = modelFolder.getFileSystem(conf)
    scala.io.Source.fromInputStream(fs.open(new Path(modelFolder, "stopwords.list"))).getLines().map(_.trim()).toSet
  }
  def loadSpotterThresholds(file: File): Seq[Double] = scala.io.Source.fromFile(file).getLines().next().split(" ").map(_.toDouble)
  def loadSpotterThresholdsFromHadoop(file: Path, conf: Configuration): Seq[Double] = {
    val fs = file.getFileSystem(conf)
    scala.io.Source.fromInputStream(fs.open(file)).getLines().next().split(" ").map(_.toDouble)
  }

  def storesFromFolder(modelFolder: File): (TokenTypeStore, SurfaceFormStore, ResourceStore, CandidateMapStore, ContextStore) = {
    val modelDataFolder = new File(modelFolder, "model")

    List(
      new File(modelDataFolder, "tokens.mem"),
      new File(modelDataFolder, "sf.mem"),
      new File(modelDataFolder, "res.mem"),
      new File(modelDataFolder, "candmap.mem")
    ).foreach {
      modelFile: File =>
        if (!modelFile.exists())
          throw new IOException("Invalid Spotlight model folder: Could not read required file %s in %s.".format(modelFile.getName, modelFile.getPath))
    }

    val quantizedCountsStore = MemoryStore.loadQuantizedCountStore(new FileInputStream(new File(modelDataFolder, "quantized_counts.mem")))

    val tokenTypeStore = MemoryStore.loadTokenTypeStore(new FileInputStream(new File(modelDataFolder, "tokens.mem")))
    val sfStore = MemoryStore.loadSurfaceFormStore(new FileInputStream(new File(modelDataFolder, "sf.mem")), quantizedCountsStore)
    val resStore = MemoryStore.loadResourceStore(new FileInputStream(new File(modelDataFolder, "res.mem")), quantizedCountsStore)
    val candMapStore = MemoryStore.loadCandidateMapStore(new FileInputStream(new File(modelDataFolder, "candmap.mem")), resStore, quantizedCountsStore)
    val contextStore = if (new File(modelDataFolder, "context.mem").exists())
      MemoryStore.loadContextStore(new FileInputStream(new File(modelDataFolder, "context.mem")), tokenTypeStore, quantizedCountsStore)
    else
      null

    (tokenTypeStore, sfStore, resStore, candMapStore, contextStore)
  }

  def storesFromHadoopFolder(modelFolder: Path, config: Configuration): (TokenTypeStore, SurfaceFormStore, ResourceStore, CandidateMapStore, ContextStore) = {
    val modelDataFolder = new Path(modelFolder, "model")
    val fileSystem = modelFolder.getFileSystem(config)

    List(
      new Path(modelDataFolder, "quantized_counts.mem"),
      new Path(modelDataFolder, "tokens.mem"),
      new Path(modelDataFolder, "sf.mem"),
      new Path(modelDataFolder, "res.mem"),
      new Path(modelDataFolder, "candmap.mem")
    ).foreach {
      modelFile: Path =>
        if (!fileSystem.exists(modelFile))
          throw new IOException("Invalid Spotlight model folder: Could not read required file %s in %s.".format(modelFile.getName, modelFile))
    }

    val quantizedCountsStore = MemoryStore.loadQuantizedCountStore(fileSystem.open(new Path(modelDataFolder, "quantized_counts.mem")))
    val tokenTypeStore = MemoryStore.loadTokenTypeStore(fileSystem.open(new Path(modelDataFolder, "tokens.mem")))
    val sfStore = MemoryStore.loadSurfaceFormStore(fileSystem.open(new Path(modelDataFolder, "sf.mem")), quantizedCountsStore)
    val resStore = MemoryStore.loadResourceStore(fileSystem.open(new Path(modelDataFolder, "res.mem")), quantizedCountsStore)
    val candMapStore = MemoryStore.loadCandidateMapStore(fileSystem.open(new Path(modelDataFolder, "candmap.mem")), resStore, quantizedCountsStore)
    val contextStore = if (fileSystem.exists(new Path(modelDataFolder, "context.mem")))
      MemoryStore.loadContextStore(fileSystem.open(new Path(modelDataFolder, "context.mem")), tokenTypeStore, quantizedCountsStore)
    else
      null

    (tokenTypeStore, sfStore, resStore, candMapStore, contextStore)
  }

  def fromFolder(modelFolder: File): SpotlightModel = {

    val (tokenTypeStore, sfStore, resStore, candMapStore, contextStore) = storesFromFolder(modelFolder)

    val stopwords = loadStopwords(modelFolder)

    val properties = new Properties()
    properties.load(new FileInputStream(new File(modelFolder, "model.properties")))

    //Read the version of the model folder. The lowest version supported by this code base is:
    val supportedVersion = 1.0
    val modelVersion = properties.getProperty("version", "0.1").toFloat
    if (modelVersion < supportedVersion)
      throw new ConfigurationException("Incompatible model version %s. This version of DBpedia Spotlight requires models of version 1.0 or newer. Please download a current model from http://spotlight.sztaki.hu/downloads/.".format(modelVersion))


    //Load the stemmer from the model file:
    def stemmer(): Stemmer = properties.getProperty("stemmer") match {
      case s: String if s equals "None" => null
      case s: String => new SnowballStemmer(s)
    }

    def contextSimilarity(): ContextSimilarity = contextStore match {
      case store:MemoryContextStore => new GenerativeContextSimilarity(tokenTypeStore, contextStore)
      case _ => new NoContextSimilarity(MathUtil.ln(1.0))
    }

    val c = properties.getProperty("opennlp_parallel", Runtime.getRuntime.availableProcessors().toString).toInt
    val cores = (1 to c)

    val tokenizer: TextTokenizer = if(new File(modelFolder, "opennlp").exists()) {

      //Create the tokenizer:
      val posTagger = new File(modelFolder, "opennlp/pos-maxent.bin")
      val tokenizerModel = new TokenizerModel(new FileInputStream(new File(modelFolder, "opennlp/token.bin")))
      val sentenceModel = new SentenceModel(new FileInputStream(new File(modelFolder, "opennlp/sent.bin")))

      def createTokenizer() = new OpenNLPTokenizer(
        new TokenizerME(tokenizerModel),
        stopwords,
        stemmer(),
        new SentenceDetectorME(sentenceModel),
        if (posTagger.exists()) new POSTaggerME(new POSModel(new FileInputStream(posTagger))) else null,
        tokenTypeStore
      ).asInstanceOf[TextTokenizer]

      if(cores.size == 1)
        createTokenizer()
      else
        new TokenizerWrapper(cores.map(_ => createTokenizer())).asInstanceOf[TextTokenizer]

    } else {
      val locale = properties.getProperty("locale").split("_")
      new LanguageIndependentTokenizer(stopwords, stemmer(), new Locale(locale(0), locale(1)), tokenTypeStore)
    }

    val searcher      = new DBCandidateSearcher(resStore, sfStore, candMapStore)
    val disambiguator = new ParagraphDisambiguatorJ(new DBTwoStepDisambiguator(
      tokenTypeStore,
      sfStore,
      resStore,
      searcher,
      new UnweightedMixture(Set("P(e)", "P(c|e)", "P(s|e)")),
      contextSimilarity()
    ))

    //If there is at least one NE model or a chunker, use the OpenNLP spotter:
    val spotter = if( new File(modelFolder, "opennlp").exists() && new File(modelFolder, "opennlp").list().exists(f => f.startsWith("ner-") || f.startsWith("chunker")) ) {
      val nerModels = new File(modelFolder, "opennlp").list().filter(_.startsWith("ner-")).map { f: String =>
        new TokenNameFinderModel(new FileInputStream(new File(new File(modelFolder, "opennlp"), f)))
      }.toList

      val chunkerFile = new File(modelFolder, "opennlp/chunker.bin")
      val chunkerModel = if (chunkerFile.exists())
        Some(new ChunkerModel(new FileInputStream(chunkerFile)))
      else
        None

      def createSpotter() = new OpenNLPSpotter(
        chunkerModel,
        nerModels,
        sfStore,
        stopwords,
        Some(loadSpotterThresholds(new File(modelFolder, "spotter_thresholds.txt")))
      ).asInstanceOf[Spotter]

      if(cores.size == 1)
        createSpotter()
      else
        new SpotterWrapper(
          cores.map(_ => createSpotter())
        ).asInstanceOf[Spotter]

    } else {
      val dict = MemoryStore.loadFSADictionary(new FileInputStream(new File(modelFolder, "fsa_dict.mem")))

      new FSASpotter(
        dict,
        sfStore,
        Some(loadSpotterThresholds(new File(modelFolder, "spotter_thresholds.txt"))),
        stopwords
      ).asInstanceOf[Spotter]
    }


    val spotters: java.util.Map[SpotterPolicy, Spotter] = Map(SpotterPolicy.SpotXmlParser -> new SpotXmlParser(), SpotterPolicy.Default -> spotter).asJava
    val disambiguators: java.util.Map[DisambiguationPolicy, ParagraphDisambiguatorJ] = Map(DisambiguationPolicy.Default -> disambiguator).asJava
    new SpotlightModel(tokenizer, spotters, disambiguators, properties)
  }

  def fromHadoopFolder(modelFolder: Path, conf: Configuration): SpotlightModel = {
    val fs = modelFolder.getFileSystem(conf)
    val (tokenTypeStore, sfStore, resStore, candMapStore, contextStore) = storesFromHadoopFolder(modelFolder, conf)

    val stopwords = loadStopwordsFromHadoop(modelFolder, conf)

    val properties = new Properties()
    properties.load(fs.open(new Path(modelFolder, "model.properties")))

    //Read the version of the model folder. The lowest version supported by this code base is:
    val supportedVersion = 1.0
    val modelVersion = properties.getProperty("version", "0.1").toFloat
    if (modelVersion < supportedVersion)
      throw new ConfigurationException("Incompatible model version %s. This version of DBpedia Spotlight requires models of version 1.0 or newer. Please download a current model from http://spotlight.sztaki.hu/downloads/.".format(modelVersion))


    //Load the stemmer from the model file:
    def stemmer(): Stemmer = properties.getProperty("stemmer") match {
      case s: String if s equals "None" => null
      case s: String => new SnowballStemmer(s)
    }

    def contextSimilarity(): ContextSimilarity = contextStore match {
      case store:MemoryContextStore => new GenerativeContextSimilarity(tokenTypeStore, contextStore)
      case _ => new NoContextSimilarity(MathUtil.ln(1.0))
    }

    val c = properties.getProperty("opennlp_parallel", Runtime.getRuntime.availableProcessors().toString).toInt
    val cores = (1 to c)

    val tokenizer: TextTokenizer = if(fs.exists(new Path(modelFolder, "opennlp"))) {

      //Create the tokenizer:
      val posTagger = new Path(modelFolder, "opennlp/pos-maxent.bin")
      val tokenizerModel = new TokenizerModel(fs.open(new Path(modelFolder, "opennlp/token.bin")))
      val sentenceModel = new SentenceModel(fs.open(new Path(modelFolder, "opennlp/sent.bin")))

      def createTokenizer() = new OpenNLPTokenizer(
        new TokenizerME(tokenizerModel),
        stopwords,
        stemmer(),
        new SentenceDetectorME(sentenceModel),
        if (fs.exists(posTagger)) new POSTaggerME(new POSModel(fs.open(posTagger))) else null,
        tokenTypeStore
      ).asInstanceOf[TextTokenizer]

      if(cores.size == 1)
        createTokenizer()
      else
        new TokenizerWrapper(cores.map(_ => createTokenizer())).asInstanceOf[TextTokenizer]

    } else {
      val locale = properties.getProperty("locale").split("_")
      new LanguageIndependentTokenizer(stopwords, stemmer(), new Locale(locale(0), locale(1)), tokenTypeStore)
    }

    val searcher      = new DBCandidateSearcher(resStore, sfStore, candMapStore)
    val disambiguator = new ParagraphDisambiguatorJ(new DBTwoStepDisambiguator(
      tokenTypeStore,
      sfStore,
      resStore,
      searcher,
      new UnweightedMixture(Set("P(e)", "P(c|e)", "P(s|e)")),
      contextSimilarity()
    ))

    //If there is at least one NE model or a chunker, use the OpenNLP spotter:
    val spotter = if( fs.exists(new Path(modelFolder, "opennlp")) && fs.listStatus(new Path(modelFolder, "opennlp")).toList.exists(f => { f.getPath.getName.startsWith("ner-") || f.getPath.getName.startsWith("chunker") })) {
      val nerModels = fs.listStatus(new Path(modelFolder, "opennlp")).toList.filter(p => p.getPath.getName.startsWith("ner-")).map { f =>
        new TokenNameFinderModel(fs.open(new Path(new Path(modelFolder, "opennlp"), f.getPath())))
      }.toList

      val chunkerFile = new Path(modelFolder, "opennlp/chunker.bin")
      val chunkerModel = if (fs.exists(chunkerFile))
        Some(new ChunkerModel(fs.open(chunkerFile)))
      else
        None

      def createSpotter() = new OpenNLPSpotter(
        chunkerModel,
        nerModels,
        sfStore,
        stopwords,
        Some(loadSpotterThresholdsFromHadoop(new Path(modelFolder, "spotter_thresholds.txt"), conf))
      ).asInstanceOf[Spotter]

      if(cores.size == 1)
        createSpotter()
      else
        new SpotterWrapper(
          cores.map(_ => createSpotter())
        ).asInstanceOf[Spotter]

    } else {
      val dict = MemoryStore.loadFSADictionary(fs.open(new Path(modelFolder, "fsa_dict.mem")))

      new FSASpotter(
        dict,
        sfStore,
        Some(loadSpotterThresholdsFromHadoop(new Path(modelFolder, "spotter_thresholds.txt"), conf)),
        stopwords
      ).asInstanceOf[Spotter]
    }


    val spotters: java.util.Map[SpotterPolicy, Spotter] = Map(SpotterPolicy.SpotXmlParser -> new SpotXmlParser(), SpotterPolicy.Default -> spotter).asJava
    val disambiguators: java.util.Map[DisambiguationPolicy, ParagraphDisambiguatorJ] = Map(DisambiguationPolicy.Default -> disambiguator).asJava
    new SpotlightModel(tokenizer, spotters, disambiguators, properties)
  }
}
