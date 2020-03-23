SELECT t1.SourceId,
       t1.SourceRoomId,
			 t1.SourcePropertyAddress,
			 t2.RSPId, t2.RSPRoomId,
			 t2.RSPPropertyAddress
FROM (   
	 SELECT
				   ROW_NUMBER() OVER(ORDER BY RoomId DESC) AS SourceId,
           RoomId SourceRoomId,
           PropertyAddress SourcePropertyAddress
      FROM TWEstate.[by].[byRoom]
     WHERE RoomRspId is NULL
       AND PropertyAddress IS NOT NULL
       AND DeleteFlag = 0
			 AND PropertyAddress like '%孙农路%') t1
 JOIN (
     SELECT 
		       ROW_NUMBER() OVER(ORDER BY RoomId DESC) AS RSPId,
           RoomId RSPRoomId,
           PropertyAddress RSPPropertyAddress
      FROM TWEstate.dbo.Room
     WHERE RoomId NOT IN (
         SELECT DISTINCT RoomRspId
                    FROM TWEstate.[by].[byRoom]
                   WHERE RoomRspId IS NOT NULL)
       AND PropertyAddress IS NOT NULL
       AND DeleteFlag = 0
			 AND PropertyAddress LIKE '%孙农路%'
 ) t2
 on t1.SourceId = t2.RSPId
 