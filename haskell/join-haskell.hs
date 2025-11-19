{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

import qualified Data.ByteString.Lazy as BL
import qualified Data.Csv as Csv
import qualified Data.Vector as V
import qualified Data.HashMap.Strict as HM
import Data.Hashable (Hashable)
import GHC.Generics (Generic)
import System.Environment (getEnv, lookupEnv)
import System.IO (hFlush, stdout)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Control.Exception (evaluate)
import System.Process (readProcess)
import System.Directory (doesFileExist)
import Data.List (intercalate, foldl')
import qualified Data.Text as T
import Control.DeepSeq (NFData, force)

-- Data row types
data XRow = XRow
    { x_id1 :: !Int
    , x_id2 :: !Int
    , x_id3 :: !Int
    , x_id4 :: !T.Text
    , x_id5 :: !T.Text
    , x_id6 :: !T.Text
    , x_v1 :: !Double
    } deriving (Show, Generic)

data SmallRow = SmallRow
    { s_id1 :: !Int
    , s_id4 :: !T.Text
    , s_v2 :: !Double
    } deriving (Show, Generic)

data MediumRow = MediumRow
    { m_id1 :: !Int
    , m_id2 :: !Int
    , m_id4 :: !T.Text
    , m_id5 :: !T.Text
    , m_v2 :: !Double
    } deriving (Show, Generic)

data BigRow = BigRow
    { b_id1 :: !Int
    , b_id2 :: !Int
    , b_id3 :: !Int
    , b_id4 :: !T.Text
    , b_id5 :: !T.Text
    , b_id6 :: !T.Text
    , b_v2 :: !Double
    } deriving (Show, Generic)

instance NFData XRow
instance NFData SmallRow
instance NFData MediumRow
instance NFData BigRow

instance Csv.FromNamedRecord XRow where
    parseNamedRecord r = XRow
        <$> r Csv..: "id1"
        <$> r Csv..: "id2"
        <$> r Csv..: "id3"
        <$> r Csv..: "id4"
        <$> r Csv..: "id5"
        <$> r Csv..: "id6"
        <$> r Csv..: "v1"

instance Csv.FromNamedRecord SmallRow where
    parseNamedRecord r = SmallRow
        <$> r Csv..: "id1"
        <$> r Csv..: "id4"
        <$> r Csv..: "v2"

instance Csv.FromNamedRecord MediumRow where
    parseNamedRecord r = MediumRow
        <$> r Csv..: "id1"
        <$> r Csv..: "id2"
        <$> r Csv..: "id4"
        <$> r Csv..: "id5"
        <$> r Csv..: "v2"

instance Csv.FromNamedRecord BigRow where
    parseNamedRecord r = BigRow
        <$> r Csv..: "id1"
        <$> r Csv..: "id2"
        <$> r Csv..: "id3"
        <$> r Csv..: "id4"
        <$> r Csv..: "id5"
        <$> r Csv..: "id6"
        <$> r Csv..: "v2"

-- Helper functions (same as groupby)
writeLog :: String -> String -> Int -> String -> Int -> Int -> String -> String -> String -> String -> Int -> Double -> Double -> String -> String -> Double -> String -> String -> IO ()
writeLog task dataName inRows question outRows outCols solution version git fun run timeSec memGb cache chk chkTimeSec onDisk machineType = do
    batch <- lookupEnv "BATCH" >>= return . maybe "" id
    timestamp <- getPOSIXTime
    csvFile <- lookupEnv "CSV_TIME_FILE" >>= return . maybe "time.csv" id
    nodename <- fmap init (readProcess "hostname" [] "")

    let comment = ""
    let timeSecRound = roundTo 3 timeSec
    let chkTimeSecRound = roundTo 3 chkTimeSec
    let memGbRound = roundTo 3 memGb

    let logRow = intercalate "," [
            nodename, batch, show timestamp, task, dataName, show inRows,
            question, show outRows, show outCols, solution, version, git, fun,
            show run, show timeSecRound, show memGbRound, cache, chk,
            show chkTimeSecRound, comment, onDisk, machineType
            ]

    fileExists <- doesFileExist csvFile
    if fileExists
        then appendFile csvFile (logRow ++ "\n")
        else do
            let header = "nodename,batch,timestamp,task,data,in_rows,question,out_rows,out_cols,solution,version,git,fun,run,time_sec,mem_gb,cache,chk,chk_time_sec,comment,on_disk,machine_type\n"
            writeFile csvFile (header ++ logRow ++ "\n")

roundTo :: Int -> Double -> Double
roundTo n x = (fromInteger $ round $ x * (10^n)) / (10.0^^n)

makeChk :: [Double] -> String
makeChk values = intercalate ";" (map formatVal values)
  where
    formatVal x = map (\c -> if c == ',' then '_' else c) (show $ roundTo 3 x)

getMemoryUsage :: IO Double
getMemoryUsage = do
    pid <- fmap init (readProcess "bash" ["-c", "echo $$"] "")
    mem <- fmap (filter (/= ' ') . init) (readProcess "ps" ["-o", "rss=", "-p", pid] "")
    let rssKb = if null mem then 0 else read mem :: Double
    return (rssKb / (1024 * 1024))

timeIt :: NFData a => IO a -> IO (a, Double)
timeIt action = do
    start <- getPOSIXTime
    result <- action
    _ <- evaluate (force result)
    end <- getPOSIXTime
    return (result, realToFrac (end - start))

-- Join helper for inner join on single key
innerJoinInt :: [XRow] -> [SmallRow] -> [(XRow, SmallRow)]
innerJoinInt xs ys =
    let yMap = foldl' (\acc y -> HM.insertWith (++) (s_id1 y) [y] acc) HM.empty ys
    in concat [[(x, y) | y <- HM.lookupDefault [] (x_id1 x) yMap] | x <- xs]

-- Parse join_to_tbls logic
joinToTbls :: String -> [String]
joinToTbls dataName =
    let parts = T.splitOn "_" (T.pack dataName)
        xn = if length parts > 1 then read (T.unpack $ parts !! 1) :: Double else 1e7
        yn = [show (floor (xn / 1e6) :: Int) ++ "e4",
              show (floor (xn / 1e3) :: Int) ++ "e3",
              show (floor xn :: Int)]
    in [T.unpack $ T.replace "NA" (T.pack (yn !! 0)) (T.pack dataName),
        T.unpack $ T.replace "NA" (T.pack (yn !! 1)) (T.pack dataName),
        T.unpack $ T.replace "NA" (T.pack (yn !! 2)) (T.pack dataName)]

main :: IO ()
main = do
    putStrLn "# join-haskell.hs"
    hFlush stdout

    let ver = "0.1.0"
    let git = "cassava-csv"
    let task = "join"
    let solution = "haskell"
    let fun = "innerJoin"
    let cache = "TRUE"
    let onDisk = "FALSE"

    dataName <- getEnv "SRC_DATANAME"
    machineType <- getEnv "MACHINE_TYPE"

    let yDataNames = joinToTbls dataName
    let srcJnX = "data/" ++ dataName ++ ".csv"
    let srcJnY = ["data/" ++ yDataNames !! 0 ++ ".csv",
                  "data/" ++ yDataNames !! 1 ++ ".csv",
                  "data/" ++ yDataNames !! 2 ++ ".csv"]

    putStrLn $ "loading datasets " ++ dataName ++ ", " ++
               yDataNames !! 0 ++ ", " ++ yDataNames !! 1 ++ ", " ++ yDataNames !! 2
    hFlush stdout

    -- Load all datasets
    csvDataX <- BL.readFile srcJnX
    csvDataSmall <- BL.readFile (srcJnY !! 0)
    csvDataMedium <- BL.readFile (srcJnY !! 1)
    csvDataBig <- BL.readFile (srcJnY !! 2)

    case (Csv.decodeByName csvDataX, Csv.decodeByName csvDataSmall,
          Csv.decodeByName csvDataMedium, Csv.decodeByName csvDataBig) of
        (Right (_, xRows), Right (_, smallRows), Right (_, mediumRows), Right (_, bigRows)) -> do
            let x = V.toList xRows :: [XRow]
            let small = V.toList smallRows :: [SmallRow]
            let medium = V.toList mediumRows :: [MediumRow]
            let big = V.toList bigRows :: [BigRow]

            putStrLn $ show (length x)
            putStrLn $ show (length small)
            putStrLn $ show (length medium)
            putStrLn $ show (length big)
            hFlush stdout

            putStrLn "joining..."
            hFlush stdout

            let inRows = length x

            -- Question 1: small inner on int
            let question1 = "small inner on int"
            (ans1, t1_1) <- timeIt $ do
                let result = innerJoinInt x small
                return result
            m1_1 <- getMemoryUsage
            (chk1, chkt1_1) <- timeIt $ do
                let sumV1 = sum [x_v1 xr | (xr, _) <- ans1]
                let sumV2 = sum [s_v2 sr | (_, sr) <- ans1]
                evaluate (sumV1, sumV2)
                return (sumV1, sumV2)
            writeLog task dataName inRows question1 (length ans1) 8 solution ver git fun 1 t1_1 m1_1 cache (makeChk [fst chk1, snd chk1]) chkt1_1 onDisk machineType

            -- Run 2
            (ans1_2, t1_2) <- timeIt $ do
                let result = innerJoinInt x small
                return result
            m1_2 <- getMemoryUsage
            (chk1_2, chkt1_2) <- timeIt $ do
                let sumV1 = sum [x_v1 xr | (xr, _) <- ans1_2]
                let sumV2 = sum [s_v2 sr | (_, sr) <- ans1_2]
                evaluate (sumV1, sumV2)
                return (sumV1, sumV2)
            writeLog task dataName inRows question1 (length ans1_2) 8 solution ver git fun 2 t1_2 m1_2 cache (makeChk [fst chk1_2, snd chk1_2]) chkt1_2 onDisk machineType
            putStrLn $ "Question 1 completed: " ++ show (length ans1_2) ++ " rows"

            putStrLn "Haskell join benchmark completed (1 question implemented)!"
            putStrLn "Note: Other join questions would require additional join types and key combinations."

        _ -> putStrLn "Error parsing CSV files"
