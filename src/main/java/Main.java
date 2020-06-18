import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

/**
 * The type Main.
 */
public class Main {

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws AnalysisException the analysis exception
     */
    public static void main(String[] args) throws AnalysisException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder()
                                                .appName("bank-analysis-spark-sql")
                                                .master("local[*]")
                                                .getOrCreate();

        Dataset<Row> basic = sparkSession.read().option("header", true).csv("files/ommlbd_basico.csv");
        Dataset<Row> empresarial = sparkSession.read().option("header", true).csv("files/ommlbd_empresarial.csv");
        Dataset<Row> familiar = sparkSession.read().option("header", true).csv("files/ommlbd_familiar.csv");
        Dataset<Row> regional = sparkSession.read().option("header", true).csv("files/ommlbd_regional.csv");
        Dataset<Row> renda = sparkSession.read().option("header", true).csv("files/ommlbd_renda.csv");

        // Joining the tables using the HS_CPF field
        Dataset<Row> rawDataset = basic
                .join(empresarial, "HS_CPF")
                .join(familiar, "HS_CPF")
                .join(regional, "HS_CPF")
                .join(renda, "HS_CPF");

        rawDataset.createTempView("raw_pessoas");

        // sanityCheck(sparkSession);

        Dataset<Row> dataset = cleanUp(rawDataset);
        dataset.createTempView("pessoas");

        // Exercício 1
        // numberOfClientsByGenderOrientation(sparkSession);

        // Exercício 2
        // minMaxEmails(sparkSession);

        // Exercício 3
        // numberOfProposesThatClientIncomeIsBiggerThan10000Reais(sparkSession);

        // Exercício 4
        // numberOfClientsThatAreFromBolsaFamiliaProgram(sparkSession);

        // Exercício 5
        // percentagesOfCredictWhoseClientHasAPublicWorker(sparkSession);

        // Exercício 6
        // percentagePerIDH(sparkSession);

        // Exercício 7
        // exercise7(sparkSession);

        // Exercício 8
        exercise8(sparkSession);

    }

    /**
     * Number of clients by gender orientation
     *
     * Results:
     *
     * |ORIENTACAO_SEXUAL|count(ORIENTACAO_SEXUAL)|
     * +-----------------+------------------------+
     * |           HETERO|                  379758|
     * |              PAN|                   11002|
     * |               BI|                   13108|
     * |             HOMO|                   35056|
     * +-----------------+------------------------+
     * */
    private static void numberOfClientsByGenderOrientation(SparkSession sparkSession) {
        Dataset<Row> result = sparkSession.sql("SELECT ORIENTACAO_SEXUAL, count(ORIENTACAO_SEXUAL) FROM pessoas GROUP BY ORIENTACAO_SEXUAL");
        result.show();
    }

    /**
     * Gets the min and max QTDEEMAIL
     *
     * Results:
     *
     * |min(QTDEMAIL)|max(QTDEMAIL)|
     * +-------------+-------------+
     * |            1|            8|
     * +-------------+-------------+
     *
     * */
    private static void minMaxEmails(SparkSession sparkSession) {
        Dataset<Row> result = sparkSession.sql("SELECT MIN(QTDEMAIL), MAX(QTDEMAIL) FROM pessoas");
        result.show();
    }

    /**
    * Number of proposes that the client has an estimate income bigger than R$10000,00
     *
     * Results:
     *
     * |count(ESTIMATIVARENDA)|
     * +----------------------+
     * |                  2719|
     * +----------------------+
    * */
    private static void numberOfProposesThatClientIncomeIsBiggerThan10000Reais(SparkSession sparkSession) {
        Dataset<Row> result = sparkSession.sql("SELECT count(ESTIMATIVARENDA) FROM pessoas WHERE ESTIMATIVARENDA > 10000");
        result.show();
    }

    /**
     * Number of clients that are from Bolsa familia
     *
     * Results:
     *
     |count(BOLSAFAMILIA)|
     +-------------------+
     |                  5|
     +-------------------+
     * */
    private static void numberOfClientsThatAreFromBolsaFamiliaProgram(SparkSession sparkSession) {
        Dataset<Row> result = sparkSession.sql("SELECT count(BOLSAFAMILIA) FROM pessoas WHERE BOLSAFAMILIA = 1");
        result.show();
    }

    /**
    * Results:
    *
    * 48,41%
    * */
    private static void percentagesOfCredictWhoseClientHasAPublicWorker(SparkSession sparkSession) {
        Dataset<Row> result = sparkSession.sql("SELECT count(FUNCIONARIOPUBLICOCASA) as total FROM pessoas");
        result.show();
        Dataset<Row> resultSet = sparkSession.sql("SELECT count(FUNCIONARIOPUBLICOCASA) as total FROM pessoas WHERE FUNCIONARIOPUBLICOCASA = 1");
        resultSet.show();
    }

    private static void percentagePerIDH(SparkSession sparkSession) {
        Dataset<Row> result = sparkSession.sql("SELECT count(IDHMUNICIPIO) as total FROM pessoas");
        Long total = result.first().getAs("total");

        Dataset<Row> resultSet = sparkSession
                .sql("SELECT count(*) FROM pessoas HAVING IDHMUNICIPIO => 0 AND IDHMUNICIPIO < 10 ");

//        "UNION " +
//                "SELECT (count(IDHMUNICIPIO)/" + total.toString() + ") as total2 FROM pessoas WHERE IDHMUNICIPIO => 10 AND IDHMUNICIPIO < 20 " +
//                "UNION " +
//                "SELECT (count(IDHMUNICIPIO)/" + total.toString() + ") as total3 FROM pessoas WHERE IDHMUNICIPIO => 20 AND IDHMUNICIPIO < 30 ");
//        resultSet.show();

        result.show();
    }

    /**
     * Number of clients that lives near a danger zone and has an income bigger than R$7k
     *
     * |count(DISTZONARISCO)|
     * +--------------------+
     * |                   1|
     * +--------------------+
     * */
    private static void exercise7(SparkSession sparkSession) {
        Dataset<Row> result = sparkSession.sql("SELECT count(DISTZONARISCO) FROM pessoas WHERE DISTZONARISCO < 5 AND ESTIMATIVARENDA > 7000");
        result.show();
    }

    /**
     * Number of proposes payed and unpayed proposals of clients that has an income bigger than 5k, grouped by target and SOCIOEMPRESA
     *
     * Results:
     *
     * |SOCIOEMPRESA|TARGET|count(TARGET)|
     * +------------+------+-------------+
     * |           1|   1.0|           43|
     * |           0|   0.0|         2973|
     * |           1|   0.0|          863|
     * |           0|   1.0|          112|
     * +------------+------+-------------+
    * */
    private static void exercise8(SparkSession sparkSession) {
        Dataset<Row> result = sparkSession.sql("SELECT SOCIOEMPRESA, TARGET, count(TARGET) FROM pessoas WHERE ESTIMATIVARENDA > 5000 GROUP BY TARGET, SOCIOEMPRESA");
        result.show();
    }

    /**
     * Sanity checks column averages.
     *
     * First 5 rows:
     *
     * Tempo CPF Average: -297.76467907883824
     * DISTCENTROCIDADE: 899.2425750243777
     * DISTZONARISCO: 58852.77625511478
     * QTDENDERECO: -127.73496094995944
     * QTDEMAIL: -128.7600974200545
     * QTDCELULAR: -128.1442163107964
     * */
    private static void sanityCheck(SparkSession sparkSession) {
        Dataset<Row> tempoCpfAverage = sparkSession.sql("select avg(TEMPOCPF) as average from raw_pessoas");
        System.out.println("Tempo CPF Average: " + tempoCpfAverage.first().getAs("average"));

        Dataset<Row> distCentroCidade = sparkSession.sql("select avg(DISTCENTROCIDADE) as average from raw_pessoas");
        System.out.println("DISTCENTROCIDADE: " + distCentroCidade.first().getAs("average"));

        Dataset<Row> DISTZONARISCO = sparkSession.sql("select avg(DISTZONARISCO) as average from raw_pessoas");
        System.out.println("DISTZONARISCO: " + DISTZONARISCO.first().getAs("average"));

        Dataset<Row> QTDENDERECO = sparkSession.sql("select avg(QTDENDERECO) as average from raw_pessoas");
        System.out.println("QTDENDERECO: " + QTDENDERECO.first().getAs("average"));

        Dataset<Row> QTDEMAIL = sparkSession.sql("select avg(QTDEMAIL) as average from raw_pessoas");
        System.out.println("QTDEMAIL: " + QTDEMAIL.first().getAs("average"));

        Dataset<Row> QTDCELULAR = sparkSession.sql("select avg(QTDCELULAR) as average from raw_pessoas");
        System.out.println("QTDCELULAR: " + QTDCELULAR.first().getAs("average"));

        Dataset<Row> CELULARPROCON = sparkSession.sql("select avg(CELULARPROCON) as average from raw_pessoas");
        System.out.println("CELULARPROCON: " + CELULARPROCON.first().getAs("average"));

        Dataset<Row> QTDFONEFIXO = sparkSession.sql("select avg(QTDFONEFIXO) as average from raw_pessoas");
        System.out.println("QTDFONEFIXO: " + QTDFONEFIXO.first().getAs("average"));

        Dataset<Row> TELFIXOPROCON = sparkSession.sql("select avg(TELFIXOPROCON) as average from raw_pessoas");
        System.out.println("TELFIXOPROCON: " + TELFIXOPROCON.first().getAs("average"));

        Dataset<Row> TARGET = sparkSession.sql("select avg(TARGET) as average from raw_pessoas");
        System.out.println("TARGET: " + TARGET.first().getAs("average"));

        Dataset<Row> ESTIMATIVARENDA = sparkSession.sql("select avg(ESTIMATIVARENDA) as average from raw_pessoas");
        System.out.println("ESTIMATIVARENDA: " + ESTIMATIVARENDA.first().getAs("average"));

        Dataset<Row> QTDDECLARACAOISENTA = sparkSession.sql("select avg(QTDDECLARACAOISENTA) as average from raw_pessoas");
        System.out.println("QTDDECLARACAOISENTA: " + QTDDECLARACAOISENTA.first().getAs("average"));

        Dataset<Row> QTDDECLARACAO10 = sparkSession.sql("select avg(QTDDECLARACAO10) as average from raw_pessoas");
        System.out.println("QTDDECLARACAO10: " + QTDDECLARACAO10.first().getAs("average"));

        Dataset<Row> QTDDECLARACAOREST10 = sparkSession.sql("select avg(QTDDECLARACAOREST10) as average from raw_pessoas");
        System.out.println("QTDDECLARACAOREST10: " + QTDDECLARACAOREST10.first().getAs("average"));

        Dataset<Row> QTDDECLARACAOPAGAR10 = sparkSession.sql("select avg(QTDDECLARACAOPAGAR10) as average from raw_pessoas");
        System.out.println("QTDDECLARACAOPAGAR10: " + QTDDECLARACAOPAGAR10.first().getAs("average"));

        Dataset<Row> RESTITUICAOAGENCIAALTARENDA = sparkSession.sql("select avg(RESTITUICAOAGENCIAALTARENDA) as average from raw_pessoas");
        System.out.println("RESTITUICAOAGENCIAALTARENDA: " + RESTITUICAOAGENCIAALTARENDA.first().getAs("average"));

        Dataset<Row> BOLSAFAMILIA = sparkSession.sql("select avg(BOLSAFAMILIA) as average from raw_pessoas");
        System.out.println("BOLSAFAMILIA: " + BOLSAFAMILIA.first().getAs("average"));

        Dataset<Row> ANOSULTIMARESTITUICAO = sparkSession.sql("select avg(ANOSULTIMARESTITUICAO) as average from raw_pessoas");
        System.out.println("ANOSULTIMARESTITUICAO: " + ANOSULTIMARESTITUICAO.first().getAs("average"));

        Dataset<Row> ANOSULTIMADECLARACAO = sparkSession.sql("select avg(ANOSULTIMADECLARACAO) as average from raw_pessoas");
        System.out.println("ANOSULTIMADECLARACAO: " + ANOSULTIMADECLARACAO.first().getAs("average"));

        Dataset<Row> ANOSULTIMADECLARACAOPAGAR = sparkSession.sql("select avg(ANOSULTIMADECLARACAOPAGAR) as average from raw_pessoas");
        System.out.println("ANOSULTIMADECLARACAOPAGAR: " + ANOSULTIMADECLARACAOPAGAR.first().getAs("average"));

        Dataset<Row> INDICEEMPREGO = sparkSession.sql("select avg(INDICEEMPREGO) as average from raw_pessoas");
        System.out.println("INDICEEMPREGO: " + INDICEEMPREGO.first().getAs("average"));

        Dataset<Row> PORTEEMPREGADOR = sparkSession.sql("select avg(PORTEEMPREGADOR) as average from raw_pessoas");
        System.out.println("PORTEEMPREGADOR: " + PORTEEMPREGADOR.first().getAs("average"));

        Dataset<Row> SOCIOEMPRESA = sparkSession.sql("select avg(SOCIOEMPRESA) as average from raw_pessoas");
        System.out.println("SOCIOEMPRESA: " + SOCIOEMPRESA.first().getAs("average"));

        Dataset<Row> FUNCIONARIOPUBLICO = sparkSession.sql("select avg(FUNCIONARIOPUBLICO) as average from raw_pessoas");
        System.out.println("FUNCIONARIOPUBLICO: " + FUNCIONARIOPUBLICO.first().getAs("average"));

        Dataset<Row> SEGMENTACAO = sparkSession.sql("select avg(SEGMENTACAO) as average from raw_pessoas");
        System.out.println("SEGMENTACAO: " + SEGMENTACAO.first().getAs("average"));

        Dataset<Row> SEGMENTACAOCOBRANCA = sparkSession.sql("select avg(SEGMENTACAOCOBRANCA) as average from raw_pessoas");
        System.out.println("SEGMENTACAOCOBRANCA: " + SEGMENTACAOCOBRANCA.first().getAs("average"));

        Dataset<Row> SEGMENTACAOECOM = sparkSession.sql("select avg(SEGMENTACAOECOM) as average from raw_pessoas");
        System.out.println("SEGMENTACAOECOM: " + SEGMENTACAOECOM.first().getAs("average"));

        Dataset<Row> SEGMENTACAOFIN = sparkSession.sql("select avg(SEGMENTACAOFIN) as average from raw_pessoas");
        System.out.println("SEGMENTACAOFIN: " + SEGMENTACAOFIN.first().getAs("average"));

        Dataset<Row> SEGMENTACAOTELECOM = sparkSession.sql("select avg(SEGMENTACAOTELECOM) as average from raw_pessoas");
        System.out.println("SEGMENTACAOTELECOM: " + SEGMENTACAOTELECOM.first().getAs("average"));

        Dataset<Row> QTDraw_pessoasCASA = sparkSession.sql("select avg(QTDraw_pessoasCASA) as average from raw_pessoas");
        System.out.println("QTDraw_pessoasCASA: " + QTDraw_pessoasCASA.first().getAs("average"));

        Dataset<Row> MENORRENDACASA = sparkSession.sql("select avg(MENORRENDACASA) as average from raw_pessoas");
        System.out.println("MENORRENDACASA: " + MENORRENDACASA.first().getAs("average"));

        Dataset<Row> MAIORRENDACASA = sparkSession.sql("select avg(MAIORRENDACASA) as average from raw_pessoas");
        System.out.println("MAIORRENDACASA: " + MAIORRENDACASA.first().getAs("average"));

        Dataset<Row> MEDIARENDACASA = sparkSession.sql("select avg(MEDIARENDACASA) as average from raw_pessoas");
        System.out.println("MEDIARENDACASA: " + MEDIARENDACASA.first().getAs("average"));

        Dataset<Row> MAIORIDADECASA = sparkSession.sql("select avg(MAIORIDADECASA) as average from raw_pessoas");
        System.out.println("MAIORIDADECASA: " + MAIORIDADECASA.first().getAs("average"));

        Dataset<Row> MENORIDADECASA = sparkSession.sql("select avg(MENORIDADECASA) as average from raw_pessoas");
        System.out.println("MENORIDADECASA: " + MENORIDADECASA.first().getAs("average"));

        Dataset<Row> MEDIAIDADECASA = sparkSession.sql("select avg(MEDIAIDADECASA) as average from raw_pessoas");
        System.out.println("MEDIAIDADECASA: " + MEDIAIDADECASA.first().getAs("average"));

        Dataset<Row> INDICMENORDEIDADE = sparkSession.sql("select avg(INDICMENORDEIDADE) as average from raw_pessoas");
        System.out.println("INDICMENORDEIDADE: " + INDICMENORDEIDADE.first().getAs("average"));

        Dataset<Row> COBRANCABAIXOCASA = sparkSession.sql("select avg(COBRANCABAIXOCASA) as average from raw_pessoas");
        System.out.println("COBRANCABAIXOCASA: " + COBRANCABAIXOCASA.first().getAs("average"));

        Dataset<Row> COBRANCAMEDIOCASA = sparkSession.sql("select avg(COBRANCAMEDIOCASA) as average from raw_pessoas");
        System.out.println("COBRANCAMEDIOCASA: " + COBRANCAMEDIOCASA.first().getAs("average"));

        Dataset<Row> COBRANCAALTACASA = sparkSession.sql("select avg(COBRANCAALTACASA) as average from raw_pessoas");
        System.out.println("COBRANCAALTACASA: " + COBRANCAALTACASA.first().getAs("average"));

        Dataset<Row> SEGMENTACAOFINBAIXACASA = sparkSession.sql("select avg(SEGMENTACAOFINBAIXACASA) as average from raw_pessoas");
        System.out.println("SEGMENTACAOFINBAIXACASA: " + SEGMENTACAOFINBAIXACASA.first().getAs("average"));

        Dataset<Row> SEGMENTACAOFINMEDIACASA = sparkSession.sql("select avg(SEGMENTACAOFINMEDIACASA) as average from raw_pessoas");
        System.out.println("SEGMENTACAOFINMEDIACASA: " + SEGMENTACAOFINMEDIACASA.first().getAs("average"));

        Dataset<Row> SEGMENTACAOALTACASA = sparkSession.sql("select avg(SEGMENTACAOALTACASA) as average from raw_pessoas");
        System.out.println("SEGMENTACAOALTACASA: " + SEGMENTACAOALTACASA.first().getAs("average"));

        Dataset<Row> BOLSAFAMILIACASA = sparkSession.sql("select avg(BOLSAFAMILIACASA) as average from raw_pessoas");
        System.out.println("BOLSAFAMILIACASA: " + BOLSAFAMILIACASA.first().getAs("average"));

        Dataset<Row> FUNCIONARIOPUBLICOCASA = sparkSession.sql("select avg(FUNCIONARIOPUBLICOCASA) as average from raw_pessoas");
        System.out.println("FUNCIONARIOPUBLICOCASA: " + FUNCIONARIOPUBLICOCASA.first().getAs("average"));

        Dataset<Row> IDADEMEDIACEP = sparkSession.sql("select avg(IDADEMEDIACEP) as average from raw_pessoas");
        System.out.println("IDADEMEDIACEP: " + IDADEMEDIACEP.first().getAs("average"));

        Dataset<Row> PERCENTMASCCEP = sparkSession.sql("select avg(PERCENTMASCCEP) as average from raw_pessoas");
        System.out.println("PERCENTMASCCEP: " + PERCENTMASCCEP.first().getAs("average"));

        Dataset<Row> PERCENTFEMCEP = sparkSession.sql("select avg(PERCENTFEMCEP) as average from raw_pessoas");
        System.out.println("PERCENTFEMCEP: " + PERCENTFEMCEP.first().getAs("average"));

        Dataset<Row> PERCENTANALFABETOCEP = sparkSession.sql("select avg(PERCENTANALFABETOCEP) as average from raw_pessoas");
        System.out.println("PERCENTANALFABETOCEP: " + PERCENTANALFABETOCEP.first().getAs("average"));

        Dataset<Row> PERCENTPRIMARIOCEP = sparkSession.sql("select avg(PERCENTPRIMARIOCEP) as average from raw_pessoas");
        System.out.println("PERCENTPRIMARIOCEP: " + PERCENTPRIMARIOCEP.first().getAs("average"));

        Dataset<Row> PERCENTFUNDAMENTALCEP = sparkSession.sql("select avg(PERCENTFUNDAMENTALCEP) as average from raw_pessoas");
        System.out.println("PERCENTFUNDAMENTALCEP: " + PERCENTFUNDAMENTALCEP.first().getAs("average"));

        Dataset<Row> PERCENTMEDIOCEP = sparkSession.sql("select avg(PERCENTMEDIOCEP) as average from raw_pessoas");
        System.out.println("PERCENTMEDIOCEP: " + PERCENTMEDIOCEP.first().getAs("average"));

        Dataset<Row> PERCENTSUPERIORCEP = sparkSession.sql("select avg(PERCENTSUPERIORCEP) as average from raw_pessoas");
        System.out.println("PERCENTSUPERIORCEP: " + PERCENTSUPERIORCEP.first().getAs("average"));

        Dataset<Row> PERCENTMESTRADOCEP = sparkSession.sql("select avg(PERCENTMESTRADOCEP) as average from raw_pessoas");
        System.out.println("PERCENTMESTRADOCEP: " + PERCENTMESTRADOCEP.first().getAs("average"));

        Dataset<Row> PERCENTDOUTORADOCEP = sparkSession.sql("select avg(PERCENTDOUTORADOCEP) as average from raw_pessoas");
        System.out.println("PERCENTDOUTORADOCEP: " + PERCENTDOUTORADOCEP.first().getAs("average"));

        Dataset<Row> PERCENTBOLSAFAMILIACEP = sparkSession.sql("select avg(PERCENTBOLSAFAMILIACEP) as average from raw_pessoas");
        System.out.println("PERCENTBOLSAFAMILIACEP: " + PERCENTBOLSAFAMILIACEP.first().getAs("average"));

        Dataset<Row> PERCENTFUNCIONARIOPUBLICOCEP = sparkSession.sql("select avg(PERCENTFUNCIONARIOPUBLICOCEP) as average from raw_pessoas");
        System.out.println("PERCENTFUNCIONARIOPUBLICOCEP: " + PERCENTFUNCIONARIOPUBLICOCEP.first().getAs("average"));

        Dataset<Row> MEDIARENDACEP = sparkSession.sql("select avg(MEDIARENDACEP) as average from raw_pessoas");
        System.out.println("MEDIARENDACEP: " + MEDIARENDACEP.first().getAs("average"));

        Dataset<Row> PIBMUNICIPIO = sparkSession.sql("select avg(PIBMUNICIPIO) as average from raw_pessoas");
        System.out.println("PIBMUNICIPIO: " + PIBMUNICIPIO.first().getAs("average"));

        Dataset<Row> QTDUTILITARIOMUNICIPIO = sparkSession.sql("select avg(QTDUTILITARIOMUNICIPIO) as average from raw_pessoas");
        System.out.println("QTDUTILITARIOMUNICIPIO: " + QTDUTILITARIOMUNICIPIO.first().getAs("average"));

        Dataset<Row> QTDAUTOMOVELMUNICIPIO = sparkSession.sql("select avg(QTDAUTOMOVELMUNICIPIO) as average from raw_pessoas");
        System.out.println("QTDAUTOMOVELMUNICIPIO: " + QTDAUTOMOVELMUNICIPIO.first().getAs("average"));

        Dataset<Row> QTDCAMINHAOMUNICIPIO = sparkSession.sql("select avg(QTDCAMINHAOMUNICIPIO) as average from raw_pessoas");
        System.out.println("QTDCAMINHAOMUNICIPIO: " + QTDCAMINHAOMUNICIPIO.first().getAs("average"));

        Dataset<Row> QTDCAMINHONETEMUNICIPIO = sparkSession.sql("select avg(QTDCAMINHONETEMUNICIPIO) as average from raw_pessoas");
        System.out.println("QTDCAMINHONETEMUNICIPIO: " + QTDCAMINHONETEMUNICIPIO.first().getAs("average"));

        Dataset<Row> QTDMOTOMUNICIPIO = sparkSession.sql("select avg(QTDMOTOMUNICIPIO) as average from raw_pessoas");
        System.out.println("QTDMOTOMUNICIPIO: " + QTDMOTOMUNICIPIO.first().getAs("average"));

        Dataset<Row> PERCENTPOPZONAURBANA = sparkSession.sql("select avg(PERCENTPOPZONAURBANA) as average from raw_pessoas");
        System.out.println("PERCENTPOPZONAURBANA: " + PERCENTPOPZONAURBANA.first().getAs("average"));

        Dataset<Row> IDHMUNICIPIO = sparkSession.sql("select avg(IDHMUNICIPIO) as average from raw_pessoas");
        System.out.println("IDHMUNICIPIO: " + IDHMUNICIPIO.first().getAs("average"));
    }

    /**
     * Clean up the datasets with found errors.
     *
     * */
    private static Dataset<Row> cleanUp(Dataset<Row> dataset) {
        return dataset
                .filter("QTDEMAIL > 0 AND ESTIMATIVARENDA > 0 AND QTDDECLARACAO10 > 0")
                .filter("QTDDECLARACAOREST10 > 0 AND QTDDECLARACAOPAGAR10 > 0")
                .filter("ANOSULTIMARESTITUICAO > 0 AND ANOSULTIMADECLARACAO > 0")
                .filter("ANOSULTIMADECLARACAOPAGAR > 0")
                .filter("BOLSAFAMILIA IN (0,1)")
                .filter("FUNCIONARIOPUBLICOCASA IN (0,1)")
                .filter("IDHMUNICIPIO > 0 AND IDHMUNICIPIO < 100")
                .filter("DISTZONARISCO > 0");
    }
}
