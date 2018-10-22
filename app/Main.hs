{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NumDecimals #-}

module Main where

import           Prelude
import           Database.Beam
import           Data.Text                          (Text)
import           Data.Int                           (Int64)

import           Database.Beam.Postgres             (Pg, PgSelectSyntax, PgCommandSyntax, Postgres, Connection)
import           Database.Beam.Postgres.Syntax      (PgExpressionSyntax)
import           Database.Beam.Backend.SQL.Types    (SqlSerial)
import           Database.Beam.Migrate.SQL          (DataType)
import           Data.Word                          (Word32)
import           Control.Monad                      (void)
import           Control.Exception                  (catch, bracket)
import           Protolude.Conv                     (toS)

import qualified Database.Beam                      as Beam
import qualified Database.Beam.Postgres             as Pg
import qualified Database.Beam.Postgres.Syntax      as Pg
import qualified Database.PostgreSQL.Simple         as PgSimple
import qualified Database.PostgreSQL.Simple.Types   as PgSimple
import           Database.Beam.Migrate.SQL.Tables
import           Database.Beam.Migrate.Types


-- Works for 'Double' fails for 'Int64'
type MyNum = Integer

-- Run with:
--      docker run -p 5432:5432 --name postgres-test -e POSTGRES_PASSWORD=test -e POSTGRES_USER=test -e POSTGRES_DB=test -d postgres:9.6
main :: IO ()
main =
    bracket setup teardown doStuff
  where
    -- Initialize: Open connection + create tables
    setup = do
        conn <- openConn
        checkedDb <- createTables conn
        return (conn, checkedDb)
    -- Clean up: Drop tables + close connection
    teardown (conn, checkedDb) = do
        dropTables conn checkedDb
        Pg.close conn
    doStuff (conn, _) = do
        store conn
        Beam.withDatabaseDebug putStrLn conn pathSumsSelect >>= print
    openConn :: IO Pg.Connection
    openConn =
        Pg.connectPostgreSQL dbUrl
    dbUrl = "host=localhost port=5432 sslmode=disable user=test password=test dbname=test connect_timeout=10"


class MyNumFieldType a where
    myNumFieldType :: DataType Pg.PgDataTypeSyntax a
instance MyNumFieldType Int64 where
    myNumFieldType = bigint
instance MyNumFieldType Double where
    myNumFieldType = double
instance MyNumFieldType Integer where
    myNumFieldType = bigint

data SomeDb f = SomeDb
    { _paths :: f (TableEntity PathT)
    } deriving Generic

instance Database be SomeDb

someDb :: DatabaseSettings be SomeDb
someDb = defaultDbSettings

data PathT f
    = Path
    { _pathId       :: Columnar f (SqlSerial Word32)
    , _pathSrc      :: Columnar f Text
    , _pathQty      :: Columnar f MyNum
    } deriving Generic

type Path = PathT Identity
type PathId = PrimaryKey PathT Identity

deriving instance Show Path
deriving instance Eq Path

instance Beamable PathT

instance Table PathT where
    data PrimaryKey PathT f = PathId
        (Columnar f (SqlSerial Word32))
            deriving Generic
    primaryKey Path{..} = PathId _pathId

instance Beamable (PrimaryKey PathT)


pathSumsSelect
    :: Pg [(Text, MyNum)]
pathSumsSelect =
    runSelectReturningList $ select newestPathSums


type PathSumQuery s numeraire slippage =
    Q PgSelectSyntax SomeDb s
        ( QExpr PgExpressionSyntax s Text
        , QExpr PgExpressionSyntax s MyNum
        )

newestPathSums :: PathSumQuery s numeraire slippage
newestPathSums =
   aggregate_
        (\path ->
            ( group_ (_pathSrc path)
            , fromMaybe_ 0 (sum_ (_pathQty path))
            )
        )
        (all_ (_paths someDb))

storePaths
    :: [(Text, MyNum)]
    -> Pg ()
storePaths paths = Beam.runInsert $
    Beam.insert (_paths someDb) $
        Beam.insertExpressions (map (toPath Beam.default_) paths)
  where
    toPath _id (sym, qty) = Path
        { _pathId   = _id
        , _pathSrc  = val_ sym
        , _pathQty  = val_ qty
        }

stuff :: [(Text, MyNum)]
stuff = [ ("ABC", 4.7e1)
        , ("ABC", 3e0)
        , ("ABC", 1.9e1)
        , ("LOL", 8e0)
        ]

store
    :: Pg.Connection
    -> IO ()
store conn =
    Beam.withDatabaseDebug putStrLn conn $ storePaths stuff

createInitial :: () -> Migration PgCommandSyntax (CheckedDatabaseSettings Postgres SomeDb)
createInitial () =
    SomeDb <$> pathTable

deleteInitial
    :: CheckedDatabaseSettings Postgres SomeDb
    -> Migration PgCommandSyntax ()
deleteInitial (SomeDb a) =
    dropTable a

pathTable
  :: Migration PgCommandSyntax (CheckedDatabaseEntity Postgres SomeDb (TableEntity PathT))
pathTable =
    createTable "paths" $ Path
        (field "id" Pg.serial unique notNull)
        (field "src" Pg.text notNull)
        (field "qty" myNumFieldType notNull)

dbCreate :: MigrationSteps PgCommandSyntax () (CheckedDatabaseSettings Postgres SomeDb)
dbCreate =
    migrationStep "Initial migration" createInitial

dbDelete
    :: CheckedDatabaseSettings Postgres SomeDb
    -> MigrationSteps PgCommandSyntax () ()
dbDelete checkedDb =
    migrationStep "Delete initial" (\_ -> deleteInitial checkedDb)

createTables :: Pg.Connection -> IO (CheckedDatabaseSettings Pg.Postgres SomeDb)
createTables conn = runMigration conn dbCreate


dropTables
    :: Pg.Connection
    -> CheckedDatabaseSettings Pg.Postgres SomeDb
    -> IO ()
dropTables conn checkedDb = runMigration conn (dbDelete checkedDb)

runMigration
    :: Connection
    -> MigrationSteps PgCommandSyntax () a -> IO a
runMigration conn migration =
    let executeFunction = tryExecute conn . newSqlQuery
        tryExecute conn query =
            catch (void $ PgSimple.execute_ conn query)
            (\err -> putStrLn ("ERROR: " ++ show (err :: PgSimple.SqlError)))
    in runMigrationSteps 0 Nothing migration
          (\_ _ -> executeMigration executeFunction)

newSqlQuery :: Pg.PgCommandSyntax -> PgSimple.Query
newSqlQuery syntax =
    PgSimple.Query (toS sqlFragment)
  where
    sqlFragment = Pg.pgRenderSyntaxScript . Pg.fromPgCommand $ syntax
