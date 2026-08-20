{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | In-process synthetic micro-benchmarks for the wave-5 work:

* keys:  Q10-shaped key materialization — n rows, ~n groups over
  3 dict-encoded text keys (built via 'dictCompactColumn', the CSV reader's
  path) + 3 Int keys; aggregate sum+count, then force ONLY the aggregate
  columns vs forcing ALL columns (the 6 gathered key columns).
* q8:    top2Snd/maximum exploded answer vs the old top2Sum checksum.
* q6:    fused median+std wiring: grouping / placement / fused kernel split,
  and fused vs median-only + std-only.
-}
import Control.Arrow ((>>>))
import Control.Exception (evaluate)
import Control.Monad (forM_, when)
import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Text.Array as A
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Data.Text.Encoding (encodeUtf8)
import qualified Data.ByteString as B
import Control.Monad.ST (runST)
import Data.Time.Clock (diffUTCTime, getCurrentTime)
import DataFrame.Functions ((.=))
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Column (..), forceColumn, fromUnboxedVector)
import DataFrame.Internal.DataFrame (
    DataFrame,
    columnNames,
    fromNamedColumns,
    getColumn,
    rowToGroup,
    valueIndices,
 )
import DataFrame.Internal.DictEncode (dictCompactColumn)
import DataFrame.Internal.Expression (AggStrategy (..), Expr (..))
import DataFrame.Internal.PackedText (mkPackedContiguous)
import qualified DataFrame.Operations.Aggregation as D
import qualified DataFrame.Operations.Core as D
import DataFrame.Operations.Merge ()
import qualified DataFrame.Operations.Subset as D
import System.Environment (getArgs)

timed :: String -> IO a -> IO (a, Double)
timed label act = do
    t0 <- getCurrentTime
    !r <- act
    t1 <- getCurrentTime
    let dt = realToFrac (diffUTCTime t1 t0) :: Double
    putStrLn (label ++ ": " ++ show dt ++ "s")
    pure (r, dt)

-- | Contiguous packed-text column: row i = pool !! (i `mod` k), then compacted
-- through 'dictCompactColumn' (the CSV reader's canonical-dict path).
mkDictTextCol :: Int -> Int -> String -> Column
mkDictTextCol n k prefix =
    let pool = [encodeUtf8 (T.pack (prefix ++ show j)) | j <- [0 .. k - 1]]
        poolLens = VU.fromList (map B.length pool)
        poolBytes = V.fromList (map B.unpack pool)
        lenAt i = VU.unsafeIndex poolLens (i `mod` k)
        offs = VU.scanl' (+) 0 (VU.generate n lenAt)
        total = VU.last offs
        arr = runST $ do
            marr <- A.new (max 1 total)
            let writeVal !pos [] = pos `seq` pure ()
                writeVal !pos (w : ws) = A.unsafeWrite marr pos w >> writeVal (pos + 1) ws
                go !i
                    | i >= n = pure ()
                    | otherwise = do
                        writeVal (VU.unsafeIndex offs i) (V.unsafeIndex poolBytes (i `mod` k))
                        go (i + 1)
            go 0
            A.unsafeFreeze marr
        contiguous = PackedText Nothing (mkPackedContiguous arr offs)
     in dictCompactColumn contiguous

forceFrameColumns :: DataFrame -> [T.Text] -> IO ()
forceFrameColumns df names =
    forM_ names $ \nm -> case getColumn nm df of
        Just c -> evaluate (forceColumn c) >> pure ()
        Nothing -> error ("missing column " ++ T.unpack nm)

benchKeys :: Int -> IO ()
benchKeys n = do
    putStrLn ("== keys (Q10 shape), n=" ++ show n ++ " ==")
    let kd1 = mkDictTextCol n 96 "id"
        kd2 = mkDictTextCol n 96 "id0"
        kd3 = mkDictTextCol n 90 "id00"
        ki4 = fromUnboxedVector (VU.generate n (\i -> i `mod` 97 :: Int))
        ki5 = fromUnboxedVector (VU.generate n (\i -> (i `div` 97) `mod` 89 :: Int))
        ki6 = fromUnboxedVector (VU.generate n (\i -> i :: Int)) -- unique => ~n groups
        v3 = fromUnboxedVector (VU.generate n (\i -> fromIntegral (i `mod` 1000) / 7 :: Double))
        df =
            fromNamedColumns
                [ ("id1", kd1)
                , ("id2", kd2)
                , ("id3", kd3)
                , ("id4", ki4)
                , ("id5", ki5)
                , ("id6", ki6)
                , ("v3", v3)
                ]
    _ <- evaluate (D.dimensions df)
    forceFrameColumns df (columnNames df) -- source columns out of the timings
    forM_ [1 :: Int, 2] $ \round_ -> do
        putStrLn ("-- round " ++ show round_)
        (g, _) <- timed "groupBy(6 keys) [force rowToGroup]" $ do
            let gg = D.groupBy ["id1", "id2", "id3", "id4", "id5", "id6"] df
            _ <- evaluate (VU.length (rowToGroup gg))
            pure gg
        (r, tAgg) <- timed "aggregate sum+count [agg cols only]" $ do
            let res =
                    D.aggregate
                        ["v3s" .= F.sum (F.col @Double "v3"), "cnt" .= F.count (F.col @Double "v3")]
                        g
            forceFrameColumns res ["v3s", "cnt"]
            pure res
        (_, tKeys) <- timed "force 6 key columns" $
            forM_ ["id1", "id2", "id3", "id4", "id5", "id6"] $ \nm ->
                timed ("  force " ++ T.unpack nm) (forceFrameColumns r [nm])
        putStrLn
            ( "forced-total/agg-only ratio: "
                ++ show ((tAgg + tKeys) / tAgg)
                ++ "  (rows=" ++ show (fst (D.dimensions r)) ++ ")"
            )

top2Sum :: Expr Double -> Expr Double
top2Sum = Agg (CollectAgg "top2Sum" f)
  where
    f v = Prelude.sum (take 2 (L.sortBy (flip compare) (V.toList v)))

top2Snd :: Expr Double -> Expr Double
top2Snd = Agg (CollectAgg "top2Snd" f)
  where
    f v = case L.sortBy (flip compare) (V.toList v) of
        (_ : b : _) -> b
        _ -> 0 / 0

chkSumDbl :: T.Text -> DataFrame -> Double
chkSumDbl col df = case D.columnAsDoubleVector (F.col @Double col) df of
    Right vec -> VU.sum vec
    Left _ -> error "chkSumDbl"

benchQ8 :: Int -> IO ()
benchQ8 n = do
    putStrLn ("== q8 (top2 explode), n=" ++ show n ++ " ==")
    let nG = n `div` 100
        id6 = fromUnboxedVector (VU.generate n (\i -> i `mod` nG :: Int))
        v3 = fromUnboxedVector (VU.generate n (\i -> fromIntegral ((i * 7919) `mod` 100000) / 100 :: Double))
        df = fromNamedColumns [("id6", id6), ("v3", v3)]
        v3e = F.col @Double "v3"
    _ <- evaluate (D.dimensions df)
    (oldAgg, _) <- timed "old top2Sum aggregate" $ do
        let r = (D.groupBy ["id6"] >>> D.aggregate ["largest2_v3" .= top2Sum v3e]) df
        _ <- evaluate (chkSumDbl "largest2_v3" r)
        pure r
    let oldChk = chkSumDbl "largest2_v3" oldAgg
    ((newRes, newAgg), _) <- timed "new max+top2Snd exploded" $ do
        let r = (D.groupBy ["id6"] >>> D.aggregate ["v3" .= F.maximum v3e, "v3b" .= top2Snd v3e]) df
            ex = D.select ["id6", "v3"] r <> D.rename "v3b" "v3" (D.select ["id6", "v3b"] r)
        _ <- evaluate (D.dimensions ex)
        forceFrameColumns ex (columnNames ex)
        pure (ex, r)
    -- Per-group bitwise check: max_g + snd_g must equal the old top2Sum_g
    -- exactly (identical accumulators; the old finalize is the single add).
    let vOld = either (error . show) id (D.columnAsDoubleVector (F.col @Double "largest2_v3") oldAgg)
        vMax = either (error . show) id (D.columnAsDoubleVector (F.col @Double "v3") newAgg)
        vSnd = either (error . show) id (D.columnAsDoubleVector (F.col @Double "v3b") newAgg)
        mismatches = VU.length (VU.filter id (VU.izipWith (\g m o -> m + VU.unsafeIndex vSnd g /= o) vMax vOld))
    putStrLn ("per-group (max+snd)==top2Sum bitwise mismatches: " ++ show mismatches)
    when (mismatches /= 0) (error "per-group top2 mismatch")
    let newChk = chkSumDbl "v3" newRes
        (rows, cols) = D.dimensions newRes
    putStrLn ("dims: " ++ show (rows, cols) ++ " (expect (" ++ show (2 * nG) ++ ",2))")
    putStrLn ("oldChk=" ++ show oldChk ++ " newChk=" ++ show newChk ++ " equal=" ++ show (oldChk == newChk))
    when (rows /= 2 * nG || cols /= 2) (error "Q8 shape mismatch")

benchQ6 :: Int -> IO ()
benchQ6 n = do
    putStrLn ("== q6 (median+std fused), n=" ++ show n ++ " ==")
    let id4 = fromUnboxedVector (VU.generate n (\i -> i `mod` 100 :: Int))
        id5 = fromUnboxedVector (VU.generate n (\i -> (i `div` 100) `mod` 100 :: Int))
        v3 = fromUnboxedVector (VU.generate n (\i -> fromIntegral ((i * 2654435761) `mod` 1000003) / 997 :: Double))
        df = fromNamedColumns [("id4", id4), ("id5", id5), ("v3", v3)]
        v3e = F.col @Double "v3"
    _ <- evaluate (D.dimensions df)
    forceFrameColumns df (columnNames df) -- source columns out of the timings
    forM_ [1 :: Int, 2, 3] $ \round_ -> do
        putStrLn ("-- round " ++ show round_)
        (g, _) <- timed "groupBy [force rowToGroup]" $ do
            let gg = D.groupBy ["id4", "id5"] df
            _ <- evaluate (VU.length (rowToGroup gg))
            pure gg
        _ <- timed "force valueIndices (placement)" $ evaluate (VU.length (valueIndices g))
        _ <- timed "aggregate median+std (fused kernel)" $ do
            let r = D.aggregate ["v3_median" .= F.median v3e, "v3_sd" .= F.stddev v3e] g
            forceFrameColumns r ["v3_median", "v3_sd"]
            pure (chkSumDbl "v3_median" r + chkSumDbl "v3_sd" r)
        _ <- timed "aggregate median only" $ do
            let r = D.aggregate ["v3_median" .= F.median v3e] g
            forceFrameColumns r ["v3_median"]
            pure (chkSumDbl "v3_median" r)
        _ <- timed "aggregate std only" $ do
            let r = D.aggregate ["v3_sd" .= F.stddev v3e] g
            forceFrameColumns r ["v3_sd"]
            pure (chkSumDbl "v3_sd" r)
        -- End-to-end (fresh grouping, like the driver's Q6)
        _ <- timed "end-to-end groupBy+aggregate median+std" $ do
            let r = (D.groupBy ["id4", "id5"] >>> D.aggregate ["v3_median" .= F.median v3e, "v3_sd" .= F.stddev v3e]) df
            forceFrameColumns r ["v3_median", "v3_sd"]
            pure (chkSumDbl "v3_median" r)
        pure ()

main :: IO ()
main = do
    args <- getArgs
    case args of
        ["keys", ns] -> benchKeys (read ns)
        ["q8", ns] -> benchQ8 (read ns)
        ["q6", ns] -> benchQ6 (read ns)
        _ -> do
            benchQ8 5000000
            benchQ6 20000000
            benchKeys 20000000
