{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

import qualified Data.ByteString.Lazy as BL
import qualified Data.Csv as Csv
import qualified Data.Vector as V
import qualified Data.HashMap.Strict as HM
import Data.Hashable (Hashable)
import GHC.Generics (Generic)
import System.Environment (getEnv, lookupEnv)
import System.IO (hFlush, stdout, hPutStrLn, stderr)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Text.Printf (printf)
import Control.Exception (evaluate)
import System.Process (readProcess)
import System.Directory (doesFileExist)
import Data.List (intercalate, foldl')
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Control.DeepSeq (NFData, force)

-- Data row type
data Row = Row
    { id1 :: !T.Text
    , id2 :: !T.Text
    , id3 :: !T.Text
    , id4 :: !Int
    , id5 :: !Int
    , id6 :: !Int
    , v1 :: !Double
    , v2 :: !Double
    , v3 :: !Double
    } deriving (Show, Generic)

instance NFData Row

instance Csv.FromNamedRecord Row where
    parseNamedRecord r = Row
        <$> r Csv..: "id1"
        <$> r Csv..: "id2"
        <$> r Csv..: "id3"
        <$> r Csv..: "id4"
        <$> r Csv..: "id5"
        <$> r Csv..: "id6"
        <$> r Csv..: "v1"
        <$> r Csv..: "v2"
        <$> r Csv..: "v3"

-- Helper functions for logging
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
            nodename,
            batch,
            show timestamp,
            task,
            dataName,
            show inRows,
            question,
            show outRows,
            show outCols,
            solution,
            version,
            git,
            fun,
            show run,
            show timeSecRound,
            show memGbRound,
            cache,
            chk,
            show chkTimeSecRound,
            comment,
            onDisk,
            machineType
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
    -- Get RSS memory in GB using ps command
    pid <- fmap init (readProcess "bash" ["-c", "echo $$"] "")
    mem <- fmap (filter (/= ' ') . init) (readProcess "ps" ["-o", "rss=", "-p", pid] "")
    let rssKb = if null mem then 0 else read mem :: Double
    return (rssKb / (1024 * 1024))  -- Convert KB to GB

-- Timing helper
timeIt :: NFData a => IO a -> IO (a, Double)
timeIt action = do
    start <- getPOSIXTime
    result <- action
    _ <- evaluate (force result)
    end <- getPOSIXTime
    return (result, realToFrac (end - start))

-- Group by helper
groupByKey :: (Eq k, Hashable k) => (a -> k) -> [a] -> HM.HashMap k [a]
groupByKey f = foldl' (\acc x -> HM.insertWith (++) (f x) [x] acc) HM.empty

-- Mean helper
mean :: [Double] -> Double
mean xs = sum xs / fromIntegral (length xs)

-- Median helper
median :: [Double] -> Double
median [] = 0
median xs = let sorted = V.fromList $ foldl' (\acc x -> x:acc) [] xs
                len = V.length sorted
            in if len `mod` 2 == 0
               then (sorted V.! (len `div` 2 - 1) + sorted V.! (len `div` 2)) / 2
               else sorted V.! (len `div` 2)

-- Standard deviation helper
stdDev :: [Double] -> Double
stdDev xs = let m = mean xs
                variance = mean [(x - m) ^ 2 | x <- xs]
            in sqrt variance

main :: IO ()
main = do
    putStrLn "# groupby-haskell.hs"
    hFlush stdout

    let ver = "0.1.0"
    let git = "cassava-csv"
    let task = "groupby"
    let solution = "haskell"
    let fun = "groupBy"
    let cache = "TRUE"
    let onDisk = "FALSE"

    dataName <- getEnv "SRC_DATANAME"
    machineType <- getEnv "MACHINE_TYPE"
    let srcFile = "data/" ++ dataName ++ ".csv"

    putStrLn $ "loading dataset " ++ dataName
    hFlush stdout

    -- Check if data has NAs (simplified check)
    let parts = T.splitOn "_" (T.pack dataName)
    let naFlag = if length parts > 3 then read (T.unpack $ parts !! 3) :: Int else 0

    if naFlag > 0
        then do
            hPutStrLn stderr "skip due to na_flag>0"
            return ()
        else do
            -- Load CSV data
            csvData <- BL.readFile srcFile
            case Csv.decodeByName csvData of
                Left err -> do
                    putStrLn $ "Error parsing CSV: " ++ err
                    return ()
                Right (_, rows) -> do
                    let x = V.toList rows :: [Row]
                    let inRows = length x
                    putStrLn $ show inRows
                    hFlush stdout

                    putStrLn "grouping..."
                    hFlush stdout

                    -- Question 1: sum v1 by id1
                    let question1 = "sum v1 by id1"
                    (ans1, t1_1) <- timeIt $ do
                        let grouped = groupByKey id1 x
                        let result = [(k, sum [v1 r | r <- rows]) | (k, rows) <- HM.toList grouped]
                        return result
                    m1_1 <- getMemoryUsage
                    (chk1, chkt1_1) <- timeIt $ evaluate $ sum [snd r | r <- ans1]
                    writeLog task dataName inRows question1 (length ans1) 2 solution ver git fun 1 t1_1 m1_1 cache (makeChk [chk1]) chkt1_1 onDisk machineType

                    -- Run 2
                    (ans1_2, t1_2) <- timeIt $ do
                        let grouped = groupByKey id1 x
                        let result = [(k, sum [v1 r | r <- rows]) | (k, rows) <- HM.toList grouped]
                        return result
                    m1_2 <- getMemoryUsage
                    (chk1_2, chkt1_2) <- timeIt $ evaluate $ sum [snd r | r <- ans1_2]
                    writeLog task dataName inRows question1 (length ans1_2) 2 solution ver git fun 2 t1_2 m1_2 cache (makeChk [chk1_2]) chkt1_2 onDisk machineType
                    putStrLn $ "Question 1 completed: " ++ show (length ans1_2) ++ " groups"

                    -- Question 2: sum v1 by id1:id2
                    let question2 = "sum v1 by id1:id2"
                    (ans2, t2_1) <- timeIt $ do
                        let grouped = groupByKey (\r -> (id1 r, id2 r)) x
                        let result = [(k, sum [v1 r | r <- rows]) | (k, rows) <- HM.toList grouped]
                        return result
                    m2_1 <- getMemoryUsage
                    (chk2, chkt2_1) <- timeIt $ evaluate $ sum [snd r | r <- ans2]
                    writeLog task dataName inRows question2 (length ans2) 3 solution ver git fun 1 t2_1 m2_1 cache (makeChk [chk2]) chkt2_1 onDisk machineType

                    -- Run 2
                    (ans2_2, t2_2) <- timeIt $ do
                        let grouped = groupByKey (\r -> (id1 r, id2 r)) x
                        let result = [(k, sum [v1 r | r <- rows]) | (k, rows) <- HM.toList grouped]
                        return result
                    m2_2 <- getMemoryUsage
                    (chk2_2, chkt2_2) <- timeIt $ evaluate $ sum [snd r | r <- ans2_2]
                    writeLog task dataName inRows question2 (length ans2_2) 3 solution ver git fun 2 t2_2 m2_2 cache (makeChk [chk2_2]) chkt2_2 onDisk machineType
                    putStrLn $ "Question 2 completed: " ++ show (length ans2_2) ++ " groups"

                    -- Question 3: sum v1 mean v3 by id3
                    let question3 = "sum v1 mean v3 by id3"
                    (ans3, t3_1) <- timeIt $ do
                        let grouped = groupByKey id3 x
                        let result = [(k, sum [v1 r | r <- rows], mean [v3 r | r <- rows]) | (k, rows) <- HM.toList grouped]
                        return result
                    m3_1 <- getMemoryUsage
                    (chk3, chkt3_1) <- timeIt $ do
                        let s1 = sum [(\(_,a,_) -> a) r | r <- ans3]
                        let s2 = sum [(\(_,_,b) -> b) r | r <- ans3]
                        evaluate (s1, s2)
                        return (s1, s2)
                    writeLog task dataName inRows question3 (length ans3) 3 solution ver git fun 1 t3_1 m3_1 cache (makeChk [fst chk3, snd chk3]) chkt3_1 onDisk machineType

                    -- Run 2
                    (ans3_2, t3_2) <- timeIt $ do
                        let grouped = groupByKey id3 x
                        let result = [(k, sum [v1 r | r <- rows], mean [v3 r | r <- rows]) | (k, rows) <- HM.toList grouped]
                        return result
                    m3_2 <- getMemoryUsage
                    (chk3_2, chkt3_2) <- timeIt $ do
                        let s1 = sum [(\(_,a,_) -> a) r | r <- ans3_2]
                        let s2 = sum [(\(_,_,b) -> b) r | r <- ans3_2]
                        evaluate (s1, s2)
                        return (s1, s2)
                    writeLog task dataName inRows question3 (length ans3_2) 3 solution ver git fun 2 t3_2 m3_2 cache (makeChk [fst chk3_2, snd chk3_2]) chkt3_2 onDisk machineType
                    putStrLn $ "Question 3 completed: " ++ show (length ans3_2) ++ " groups"

                    -- Question 4: mean v1:v3 by id4
                    let question4 = "mean v1:v3 by id4"
                    (ans4, t4_1) <- timeIt $ do
                        let grouped = groupByKey id4 x
                        let result = [(k, mean [v1 r | r <- rows], mean [v2 r | r <- rows], mean [v3 r | r <- rows]) | (k, rows) <- HM.toList grouped]
                        return result
                    m4_1 <- getMemoryUsage
                    (chk4, chkt4_1) <- timeIt $ do
                        let s1 = sum [(\(_,a,_,_) -> a) r | r <- ans4]
                        let s2 = sum [(\(_,_,b,_) -> b) r | r <- ans4]
                        let s3 = sum [(\(_,_,_,c) -> c) r | r <- ans4]
                        evaluate (s1, s2, s3)
                        return (s1, s2, s3)
                    writeLog task dataName inRows question4 (length ans4) 4 solution ver git fun 1 t4_1 m4_1 cache (makeChk [(\(a,_,_) -> a) chk4, (\(_,b,_) -> b) chk4, (\(_,_,c) -> c) chk4]) chkt4_1 onDisk machineType

                    -- Run 2
                    (ans4_2, t4_2) <- timeIt $ do
                        let grouped = groupByKey id4 x
                        let result = [(k, mean [v1 r | r <- rows], mean [v2 r | r <- rows], mean [v3 r | r <- rows]) | (k, rows) <- HM.toList grouped]
                        return result
                    m4_2 <- getMemoryUsage
                    (chk4_2, chkt4_2) <- timeIt $ do
                        let s1 = sum [(\(_,a,_,_) -> a) r | r <- ans4_2]
                        let s2 = sum [(\(_,_,b,_) -> b) r | r <- ans4_2]
                        let s3 = sum [(\(_,_,_,c) -> c) r | r <- ans4_2]
                        evaluate (s1, s2, s3)
                        return (s1, s2, s3)
                    writeLog task dataName inRows question4 (length ans4_2) 4 solution ver git fun 2 t4_2 m4_2 cache (makeChk [(\(a,_,_) -> a) chk4_2, (\(_,b,_) -> b) chk4_2, (\(_,_,c) -> c) chk4_2]) chkt4_2 onDisk machineType
                    putStrLn $ "Question 4 completed: " ++ show (length ans4_2) ++ " groups"

                    -- Question 5: sum v1:v3 by id6
                    let question5 = "sum v1:v3 by id6"
                    (ans5, t5_1) <- timeIt $ do
                        let grouped = groupByKey id6 x
                        let result = [(k, sum [v1 r | r <- rows], sum [v2 r | r <- rows], sum [v3 r | r <- rows]) | (k, rows) <- HM.toList grouped]
                        return result
                    m5_1 <- getMemoryUsage
                    (chk5, chkt5_1) <- timeIt $ do
                        let s1 = sum [(\(_,a,_,_) -> a) r | r <- ans5]
                        let s2 = sum [(\(_,_,b,_) -> b) r | r <- ans5]
                        let s3 = sum [(\(_,_,_,c) -> c) r | r <- ans5]
                        evaluate (s1, s2, s3)
                        return (s1, s2, s3)
                    writeLog task dataName inRows question5 (length ans5) 4 solution ver git fun 1 t5_1 m5_1 cache (makeChk [(\(a,_,_) -> a) chk5, (\(_,b,_) -> b) chk5, (\(_,_,c) -> c) chk5]) chkt5_1 onDisk machineType

                    -- Run 2
                    (ans5_2, t5_2) <- timeIt $ do
                        let grouped = groupByKey id6 x
                        let result = [(k, sum [v1 r | r <- rows], sum [v2 r | r <- rows], sum [v3 r | r <- rows]) | (k, rows) <- HM.toList grouped]
                        return result
                    m5_2 <- getMemoryUsage
                    (chk5_2, chkt5_2) <- timeIt $ do
                        let s1 = sum [(\(_,a,_,_) -> a) r | r <- ans5_2]
                        let s2 = sum [(\(_,_,b,_) -> b) r | r <- ans5_2]
                        let s3 = sum [(\(_,_,_,c) -> c) r | r <- ans5_2]
                        evaluate (s1, s2, s3)
                        return (s1, s2, s3)
                    writeLog task dataName inRows question5 (length ans5_2) 4 solution ver git fun 2 t5_2 m5_2 cache (makeChk [(\(a,_,_) -> a) chk5_2, (\(_,b,_) -> b) chk5_2, (\(_,_,c) -> c) chk5_2]) chkt5_2 onDisk machineType
                    putStrLn $ "Question 5 completed: " ++ show (length ans5_2) ++ " groups"

                    putStrLn "Haskell groupby benchmark completed (5 questions implemented)!"
                    putStrLn "Note: Questions 6-10 would require median, regression, and top-n functions."
