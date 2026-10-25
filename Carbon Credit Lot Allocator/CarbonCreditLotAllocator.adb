with Ada.Command_Line;
with Ada.Containers.Generic_Array_Sort;
with Ada.Exceptions;
with Ada.Containers.Indefinite_Hashed_Sets;
with Ada.Strings.Fixed;
with Ada.Strings.Hash;
with Ada.Text_IO;

--  Allocates whole tonne carbon credit serial ranges from issued lots to buyer
--  orders. Every output serial is traceable to exactly one order, no serial is
--  handed out twice, and the arithmetic is integer only so no tonne is lost to
--  rounding. Input is CSV on a file or stdin. Run with --help for the grammar.

procedure CarbonCreditLotAllocator is

   subtype Big is Long_Long_Integer;

   Max_Lots        : constant := 50_000;
   Max_Orders      : constant := 50_000;
   Max_Fills       : constant := Max_Lots + Max_Orders;
   Max_Line        : constant := 4_096;
   Max_Id_Length   : constant := 64;
   Max_Serial      : constant Big := 1_000_000_000_000_000;
   Max_Total_Units : constant Big := 2_000_000_000;
   Max_Buffer_Bps  : constant Big := 5_000;
   Min_Vintage     : constant Big := 1_990;
   Max_Vintage     : constant Big := 2_100;
   Max_Priority    : constant Big := 1_000;

   Input_Error : exception;
   Check_Error : exception;
   Help_Requested : exception;

   type Policy_Kind is (Fifo, Prorata);
   type Vintage_Pref is (Oldest, Newest);

   Policy        : Policy_Kind := Fifo;
   Vintage_Order : Vintage_Pref := Oldest;
   Buffer_Bps    : Big := 0;
   Want_Json     : Boolean := False;
   Fail_Short    : Boolean := False;
   Source_Name   : String (1 .. 4_096) := (others => ' ');
   Source_Len    : Natural := 0;

   --  Exit codes: 0 ok, 1 internal check failed, 2 bad input or
   --  usage, 3 shortfall while --fail-on-shortfall is set.
   Exit_Internal  : constant Ada.Command_Line.Exit_Status := 1;
   Exit_Bad_Input : constant Ada.Command_Line.Exit_Status := 2;
   Exit_Shortfall : constant Ada.Command_Line.Exit_Status := 3;

   type Lot_Rec is record
      Id         : String (1 .. Max_Id_Length) := (others => ' ');
      Id_Len     : Natural := 0;
      Vintage    : Big := 0;
      First      : Big := 0;
      Last       : Big := 0;
      Withheld   : Big := 0;
      Next       : Big := 0;
      Avail_Last : Big := 0;
      Allocated  : Big := 0;
      Line       : Natural := 0;
   end record;

   type Order_Rec is record
      Id          : String (1 .. Max_Id_Length) := (others => ' ');
      Id_Len      : Natural := 0;
      Quantity    : Big := 0;
      Min_Vintage : Big := 0;
      Priority    : Big := 0;
      Entitled    : Big := 0;
      Allocated   : Big := 0;
      Line        : Natural := 0;
   end record;

   type Fill_Rec is record
      Order : Positive := 1;
      Lot   : Positive := 1;
      First : Big := 0;
      Last  : Big := 0;
   end record;

   type Lot_Array is array (Positive range <>) of Lot_Rec;
   type Order_Array is array (Positive range <>) of Order_Rec;
   type Fill_Array is array (Positive range <>) of Fill_Rec;
   type Lot_Array_Ptr is access Lot_Array;
   type Order_Array_Ptr is access Order_Array;
   type Fill_Array_Ptr is access Fill_Array;

   Lots   : constant Lot_Array_Ptr := new Lot_Array (1 .. Max_Lots);
   Orders : constant Order_Array_Ptr := new Order_Array (1 .. Max_Orders);
   Fills  : constant Fill_Array_Ptr := new Fill_Array (1 .. Max_Fills);

   Lot_Count   : Natural := 0;
   Order_Count : Natural := 0;
   Fill_Count  : Natural := 0;

   package Id_Sets is new Ada.Containers.Indefinite_Hashed_Sets
     (Element_Type        => String,
      Hash                => Ada.Strings.Hash,
      Equivalent_Elements => "=");

   --  One sort key type serves every ordering. Keys ascend, then the original
   --  index breaks ties so results never depend on the sort algorithm.
   type Sort_Item is record
      K1  : Big;
      K2  : Big;
      Idx : Positive;
   end record;

   type Sort_Array is array (Positive range <>) of Sort_Item;

   function Less (A, B : Sort_Item) return Boolean is
   begin
      if A.K1 /= B.K1 then
         return A.K1 < B.K1;
      elsif A.K2 /= B.K2 then
         return A.K2 < B.K2;
      else
         return A.Idx < B.Idx;
      end if;
   end Less;

   procedure Sort is new Ada.Containers.Generic_Array_Sort
     (Index_Type   => Positive,
      Element_Type => Sort_Item,
      Array_Type   => Sort_Array,
      "<"          => Less);

   type Index_Array is array (Positive range <>) of Positive;

   function Img (N : Big) return String is
     (Ada.Strings.Fixed.Trim (Big'Image (N), Ada.Strings.Both));

   function Lot_Name (L : Lot_Rec) return String is (L.Id (1 .. L.Id_Len));
   function Order_Name (O : Order_Rec) return String is (O.Id (1 .. O.Id_Len));

   procedure Fail (Line : Natural; Message : String) is
   begin
      if Line = 0 then
         raise Input_Error with Message;
      else
         raise Input_Error with "line " & Img (Big (Line)) & ": " & Message;
      end if;
   end Fail;

   procedure Usage is
      use Ada.Text_IO;
   begin
      Put_Line ("usage: CarbonCreditLotAllocator [options] <file | ->");
      Put_Line ("  --policy fifo|prorata     fifo fills orders in priority order (default)");
      Put_Line ("                            prorata splits scarce supply by largest remainder");
      Put_Line ("  --vintage oldest|newest   which lots are drawn first (default oldest)");
      Put_Line ("  --buffer-bps N            withhold ceil(N/10000) of every lot, 0 to 5000");
      Put_Line ("  --json                    print JSON instead of CSV lines");
      Put_Line ("  --fail-on-shortfall       exit 3 when any order is not fully filled");
      Put_Line ("  --help                    this text");
      New_Line;
      Put_Line ("input, one record per line, # starts a comment:");
      Put_Line ("  LOT,<id>,<vintage>,<first_serial>,<last_serial>");
      Put_Line ("  ORDER,<id>,<tonnes>[,<min_vintage>[,<priority>]]");
      Put_Line ("ids use letters, digits, dot, dash and underscore, up to 64 characters.");
      Put_Line ("one serial equals one tonne. serials are inclusive and may not overlap.");
      New_Line;
      Put_Line ("exit codes: 0 ok, 1 internal check failed, 2 bad input, 3 shortfall");
   end Usage;

   function Valid_Id (S : String) return Boolean is
   begin
      if S'Length = 0 or else S'Length > Max_Id_Length then
         return False;
      end if;
      for C of S loop
         if not (C in 'A' .. 'Z' or else C in 'a' .. 'z' or else C in '0' .. '9'
                 or else C = '.' or else C = '-' or else C = '_')
         then
            return False;
         end if;
      end loop;
      return True;
   end Valid_Id;

   --  Digits only, no sign, no separators, bounded so Big cannot overflow.
   function Parse_Number
     (Text : String; Line : Natural; Name : String; Low, High : Big) return Big
   is
      Value : Big := 0;
   begin
      if Text'Length = 0 then
         Fail (Line, Name & " is empty");
      elsif Text'Length > 18 then
         Fail (Line, Name & " is too long to be a valid number");
      end if;
      for C of Text loop
         if C not in '0' .. '9' then
            Fail (Line, Name & " must be a plain non negative integer, got '" & Text & "'");
         end if;
         Value := Value * 10 + Big (Character'Pos (C) - Character'Pos ('0'));
      end loop;
      if Value < Low or else Value > High then
         Fail (Line, Name & " must be between " & Img (Low) & " and " & Img (High)
                     & ", got " & Img (Value));
      end if;
      return Value;
   end Parse_Number;

   type Field_Bounds is record
      First : Positive;
      Last  : Natural;
   end record;
   type Field_Table is array (1 .. 8) of Field_Bounds;

   --  Splits on commas and trims blanks. Reports one more than the table size
   --  when a line carries too many fields so the caller can reject it.
   procedure Split (Line : String; Table : out Field_Table; Count : out Natural) is
      Start : Positive := Line'First;
      Stop  : Natural;

      procedure Add (From, To : Integer) is
         F : Integer := From;
         T : Integer := To;
      begin
         while F <= T and then (Line (F) = ' ' or else Line (F) = ASCII.HT) loop
            F := F + 1;
         end loop;
         while T >= F and then (Line (T) = ' ' or else Line (T) = ASCII.HT) loop
            T := T - 1;
         end loop;
         Count := Count + 1;
         if Count <= Table'Last then
            Table (Count) := (if F > T then (First => 1, Last => 0) else (First => F, Last => T));
         end if;
      end Add;
   begin
      Count := 0;
      Table := (others => (First => 1, Last => 0));
      if Line'Length = 0 then
         return;
      end if;
      loop
         Stop := Start;
         while Stop <= Line'Last and then Line (Stop) /= ',' loop
            Stop := Stop + 1;
         end loop;
         Add (Start, Stop - 1);
         exit when Stop > Line'Last;
         Start := Stop + 1;
      end loop;
   end Split;

   procedure Check_Id
     (Seen : in out Id_Sets.Set; Id : String; Kind : String; Line : Natural)
   is
      Pos      : Id_Sets.Cursor;
      Inserted : Boolean;
   begin
      if not Valid_Id (Id) then
         Fail (Line, Kind & " id '" & Id
                     & "' must be 1 to 64 characters of letters, digits, dot, dash or underscore");
      end if;
      Seen.Insert (Id, Pos, Inserted);
      if not Inserted then
         Fail (Line, "duplicate " & Kind & " id '" & Id & "'");
      end if;
   end Check_Id;

   procedure Parse_Input is
      use Ada.Text_IO;
      Buffer      : String (1 .. Max_Line);
      Last        : Natural;
      Line_No     : Natural := 0;
      Lot_Ids     : Id_Sets.Set;
      Order_Ids   : Id_Sets.Set;
      Table       : Field_Table;
      Count       : Natural;
      Total_Lots  : Big := 0;
      Total_Order : Big := 0;

      function F (K : Positive) return String is
      begin
         if K > Count or else Table (K).Last < Table (K).First then
            return "";
         end if;
         return Buffer (Table (K).First .. Table (K).Last);
      end F;

      function Upper (S : String) return String is
         R : String := S;
      begin
         for C of R loop
            if C in 'a' .. 'z' then
               C := Character'Val (Character'Pos (C) - 32);
            end if;
         end loop;
         return R;
      end Upper;
   begin
      while not End_Of_File loop
         Get_Line (Buffer, Last);
         Line_No := Line_No + 1;
         if Last = Buffer'Last and then not End_Of_Line and then not End_Of_File then
            Fail (Line_No, "line is longer than " & Img (Max_Line) & " characters");
         end if;
         if Last > 0 and then Buffer (Last) = ASCII.CR then
            Last := Last - 1;
         end if;
         if Line_No = 1 and then Last >= 3
           and then Buffer (1) = Character'Val (16#EF#)
           and then Buffer (2) = Character'Val (16#BB#)
           and then Buffer (3) = Character'Val (16#BF#)
         then
            Fail (Line_No, "input starts with a UTF-8 byte order mark, remove it");
         end if;

         declare
            Text  : constant String := Buffer (1 .. Last);
            Blank : Boolean := True;
            Hash  : Boolean := False;
         begin
            for C of Text loop
               if C = '#' then
                  Hash := True;
                  exit;
               elsif C /= ' ' and then C /= ASCII.HT then
                  Blank := False;
                  exit;
               end if;
            end loop;
            if Blank or else Hash then
               goto Continue;
            end if;
         end;

         for I in 1 .. Last loop
            if Buffer (I) = '"' then
               Fail (Line_No, "quoted fields are not supported, ids and numbers never need quotes");
            end if;
         end loop;

         Split (Buffer (1 .. Last), Table, Count);
         if Count > Table'Last then
            Fail (Line_No, "too many fields");
         end if;

         declare
            Kind : constant String := Upper (F (1));
         begin
            if Kind = "LOT" then
               if Count /= 5 then
                  Fail (Line_No, "LOT needs exactly: LOT,id,vintage,first_serial,last_serial");
               end if;
               if Lot_Count = Max_Lots then
                  Fail (Line_No, "more than " & Img (Max_Lots) & " lots");
               end if;
               Check_Id (Lot_Ids, F (2), "lot", Line_No);
               Lot_Count := Lot_Count + 1;
               declare
                  L : Lot_Rec renames Lots (Lot_Count);
                  Name : constant String := F (2);
               begin
                  L.Id (1 .. Name'Length) := Name;
                  L.Id_Len := Name'Length;
                  L.Line := Line_No;
                  L.Vintage := Parse_Number (F (3), Line_No, "vintage", Min_Vintage, Max_Vintage);
                  L.First := Parse_Number (F (4), Line_No, "first_serial", 1, Max_Serial);
                  L.Last := Parse_Number (F (5), Line_No, "last_serial", 1, Max_Serial);
                  if L.Last < L.First then
                     Fail (Line_No, "last_serial " & Img (L.Last) & " is below first_serial "
                                    & Img (L.First));
                  end if;
                  Total_Lots := Total_Lots + (L.Last - L.First + 1);
                  if Total_Lots > Max_Total_Units then
                     Fail (Line_No, "total lot supply exceeds " & Img (Max_Total_Units)
                                    & " tonnes, split the run");
                  end if;
               end;

            elsif Kind = "ORDER" then
               if Count < 3 or else Count > 5 then
                  Fail (Line_No, "ORDER needs: ORDER,id,tonnes[,min_vintage[,priority]]");
               end if;
               if Order_Count = Max_Orders then
                  Fail (Line_No, "more than " & Img (Max_Orders) & " orders");
               end if;
               Check_Id (Order_Ids, F (2), "order", Line_No);
               Order_Count := Order_Count + 1;
               declare
                  O : Order_Rec renames Orders (Order_Count);
                  Name : constant String := F (2);
               begin
                  O.Id (1 .. Name'Length) := Name;
                  O.Id_Len := Name'Length;
                  O.Line := Line_No;
                  O.Quantity := Parse_Number (F (3), Line_No, "tonnes", 1, Max_Total_Units);
                  O.Min_Vintage :=
                    (if F (4) = "" then 0
                     else Parse_Number (F (4), Line_No, "min_vintage", Min_Vintage, Max_Vintage));
                  O.Priority :=
                    (if F (5) = "" then 0
                     else Parse_Number (F (5), Line_No, "priority", 0, Max_Priority));
                  Total_Order := Total_Order + O.Quantity;
                  if Total_Order > Max_Total_Units then
                     Fail (Line_No, "total demand exceeds " & Img (Max_Total_Units)
                                    & " tonnes, split the run");
                  end if;
               end;

            else
               Fail (Line_No, "unknown record type '" & F (1) & "', expected LOT or ORDER");
            end if;
         end;
         <<Continue>>
      end loop;

      if Lot_Count = 0 then
         Fail (0, "input has no LOT records");
      end if;
      if Order_Count = 0 then
         Fail (0, "input has no ORDER records");
      end if;
   end Parse_Input;

   --  Two lots sharing a serial would let one tonne be sold twice, so this is
   --  a hard input error rather than a warning.
   procedure Reject_Overlapping_Lots is
      Items : Sort_Array (1 .. Lot_Count);
   begin
      for I in 1 .. Lot_Count loop
         Items (I) := (K1 => Lots (I).First, K2 => Lots (I).Last, Idx => I);
      end loop;
      Sort (Items);
      for I in 2 .. Lot_Count loop
         declare
            Prev : Lot_Rec renames Lots (Items (I - 1).Idx);
            Cur  : Lot_Rec renames Lots (Items (I).Idx);
         begin
            if Cur.First <= Prev.Last then
               Fail (Cur.Line,
                     "lot '" & Lot_Name (Cur) & "' serials " & Img (Cur.First) & " to "
                     & Img (Cur.Last) & " overlap lot '" & Lot_Name (Prev) & "' serials "
                     & Img (Prev.First) & " to " & Img (Prev.Last));
            end if;
         end;
      end loop;
   end Reject_Overlapping_Lots;

   Total_Supply   : Big := 0;
   Total_Withheld : Big := 0;
   Total_Demand   : Big := 0;

   --  The buffer share comes off the tail of each lot and rounds up, so the
   --  pool is never short by a fraction of a tonne.
   procedure Apply_Buffer is
   begin
      for I in 1 .. Lot_Count loop
         declare
            L    : Lot_Rec renames Lots (I);
            Size : constant Big := L.Last - L.First + 1;
         begin
            L.Withheld := (Size * Buffer_Bps + 9_999) / 10_000;
            L.Next := L.First;
            L.Avail_Last := L.Last - L.Withheld;
            Total_Withheld := Total_Withheld + L.Withheld;
            Total_Supply := Total_Supply + (Size - L.Withheld);
         end;
      end loop;
      for I in 1 .. Order_Count loop
         Total_Demand := Total_Demand + Orders (I).Quantity;
      end loop;
   end Apply_Buffer;

   --  Hamilton largest remainder: floor shares first, then one extra tonne to
   --  each order with the biggest remainder until supply is spent. Products
   --  stay below 4e18 because both totals are capped at 2e9.
   procedure Compute_Entitlements is
      Rem_Items : Sort_Array (1 .. Order_Count);
      Handed    : Big := 0;
      Spare     : Big;
   begin
      if Policy = Fifo or else Total_Demand <= Total_Supply then
         for I in 1 .. Order_Count loop
            Orders (I).Entitled := Orders (I).Quantity;
         end loop;
         return;
      end if;
      for I in 1 .. Order_Count loop
         declare
            Num : constant Big := Orders (I).Quantity * Total_Supply;
         begin
            Orders (I).Entitled := Num / Total_Demand;
            Handed := Handed + Orders (I).Entitled;
            Rem_Items (I) := (K1 => -(Num mod Total_Demand), K2 => 0, Idx => I);
         end;
      end loop;
      Sort (Rem_Items);
      Spare := Total_Supply - Handed;
      for K in 1 .. Order_Count loop
         exit when Spare = 0;
         declare
            O : Order_Rec renames Orders (Rem_Items (K).Idx);
         begin
            if O.Entitled < O.Quantity then
               O.Entitled := O.Entitled + 1;
               Spare := Spare - 1;
            end if;
         end;
      end loop;
   end Compute_Entitlements;

   procedure Allocate is
      Lot_Keys   : Sort_Array (1 .. Lot_Count);
      Order_Keys : Sort_Array (1 .. Order_Count);
      Lot_Seq    : Index_Array (1 .. Lot_Count);

      procedure Draw (O : in out Order_Rec; Order_Idx : Positive) is
         Need : Big := O.Entitled;
      begin
         for K in 1 .. Lot_Count loop
            exit when Need = 0;
            declare
               L     : Lot_Rec renames Lots (Lot_Seq (K));
               Avail : constant Big := L.Avail_Last - L.Next + 1;
               Take  : Big;
            begin
               if L.Vintage >= O.Min_Vintage and then Avail > 0 then
                  Take := Big'Min (Need, Avail);
                  Fill_Count := Fill_Count + 1;
                  Fills (Fill_Count) :=
                    (Order => Order_Idx, Lot => Lot_Seq (K),
                     First => L.Next, Last => L.Next + Take - 1);
                  L.Next := L.Next + Take;
                  L.Allocated := L.Allocated + Take;
                  O.Allocated := O.Allocated + Take;
                  Need := Need - Take;
               end if;
            end;
         end loop;
      end Draw;
   begin
      for I in 1 .. Lot_Count loop
         Lot_Keys (I) :=
           (K1 => (if Vintage_Order = Oldest then Lots (I).Vintage else -Lots (I).Vintage),
            K2 => Lots (I).First, Idx => I);
      end loop;
      Sort (Lot_Keys);
      for I in 1 .. Lot_Count loop
         Lot_Seq (I) := Lot_Keys (I).Idx;
      end loop;

      for I in 1 .. Order_Count loop
         Order_Keys (I) :=
           (K1 => -Orders (I).Priority, K2 => -Orders (I).Min_Vintage, Idx => I);
      end loop;
      Sort (Order_Keys);
      for K in 1 .. Order_Count loop
         Draw (Orders (Order_Keys (K).Idx), Order_Keys (K).Idx);
      end loop;
   end Allocate;

   --  Independent re check of the result. If this ever fails the allocator has
   --  a bug and the output must not be trusted, hence a separate exit code.
   procedure Self_Check is
      Fill_Sum  : Big := 0;
      Order_Sum : Big := 0;
      Lot_Sum   : Big := 0;
      End_Seen  : array (1 .. Lot_Count) of Big := (others => 0);
      Per_Order : array (1 .. Order_Count) of Big := (others => 0);
   begin
      for K in 1 .. Fill_Count loop
         declare
            F : Fill_Rec renames Fills (K);
            L : Lot_Rec renames Lots (F.Lot);
         begin
            if F.First < L.First or else F.Last > L.Avail_Last or else F.Last < F.First then
               raise Check_Error with "fill " & Img (Big (K)) & " leaves lot " & Lot_Name (L);
            end if;
            if F.First <= End_Seen (F.Lot) then
               raise Check_Error with "serial reused inside lot " & Lot_Name (L);
            end if;
            End_Seen (F.Lot) := F.Last;
            Fill_Sum := Fill_Sum + (F.Last - F.First + 1);
            Per_Order (F.Order) := Per_Order (F.Order) + (F.Last - F.First + 1);
            if Orders (F.Order).Min_Vintage > L.Vintage then
               raise Check_Error with "order " & Order_Name (Orders (F.Order))
                 & " received a vintage below its minimum";
            end if;
         end;
      end loop;
      for I in 1 .. Order_Count loop
         Order_Sum := Order_Sum + Orders (I).Allocated;
         if Per_Order (I) /= Orders (I).Allocated
           or else Orders (I).Allocated > Orders (I).Entitled
           or else Orders (I).Entitled > Orders (I).Quantity
         then
            raise Check_Error with "order " & Order_Name (Orders (I)) & " totals disagree";
         end if;
      end loop;
      for I in 1 .. Lot_Count loop
         Lot_Sum := Lot_Sum + Lots (I).Allocated;
         if Lots (I).Allocated + Lots (I).Withheld > Lots (I).Last - Lots (I).First + 1 then
            raise Check_Error with "lot " & Lot_Name (Lots (I)) & " is over allocated";
         end if;
      end loop;
      if Fill_Sum /= Order_Sum or else Fill_Sum /= Lot_Sum then
         raise Check_Error with "fill, order and lot totals disagree";
      end if;
   end Self_Check;

   function Total_Allocated return Big is
      S : Big := 0;
   begin
      for I in 1 .. Order_Count loop
         S := S + Orders (I).Allocated;
      end loop;
      return S;
   end Total_Allocated;

   procedure Report_Csv is
      use Ada.Text_IO;
   begin
      Put_Line ("POLICY," & (if Policy = Fifo then "fifo" else "prorata") & ","
                & (if Vintage_Order = Oldest then "oldest" else "newest") & ","
                & Img (Buffer_Bps));
      Put_Line ("FILL,order,lot,vintage,first_serial,last_serial,tonnes");
      for K in 1 .. Fill_Count loop
         declare
            F : Fill_Rec renames Fills (K);
         begin
            Put_Line ("FILL," & Order_Name (Orders (F.Order)) & "," & Lot_Name (Lots (F.Lot)) & ","
                      & Img (Lots (F.Lot).Vintage) & "," & Img (F.First) & "," & Img (F.Last) & ","
                      & Img (F.Last - F.First + 1));
         end;
      end loop;
      Put_Line ("ORDER,id,requested,entitled,allocated,shortfall");
      for I in 1 .. Order_Count loop
         declare
            O : Order_Rec renames Orders (I);
         begin
            Put_Line ("ORDER," & Order_Name (O) & "," & Img (O.Quantity) & "," & Img (O.Entitled)
                      & "," & Img (O.Allocated) & "," & Img (O.Quantity - O.Allocated));
         end;
      end loop;
      Put_Line ("LOT,id,vintage,size,withheld,allocated,remaining");
      for I in 1 .. Lot_Count loop
         declare
            L    : Lot_Rec renames Lots (I);
            Size : constant Big := L.Last - L.First + 1;
         begin
            Put_Line ("LOT," & Lot_Name (L) & "," & Img (L.Vintage) & "," & Img (Size) & ","
                      & Img (L.Withheld) & "," & Img (L.Allocated) & ","
                      & Img (Size - L.Withheld - L.Allocated));
         end;
      end loop;
      Put_Line ("TOTAL,supply,withheld,demand,allocated,shortfall");
      Put_Line ("TOTAL," & Img (Total_Supply) & "," & Img (Total_Withheld) & ","
                & Img (Total_Demand) & "," & Img (Total_Allocated) & ","
                & Img (Total_Demand - Total_Allocated));
      Put_Line ("STATUS," & (if Total_Allocated = Total_Demand then "FILLED" else "SHORTFALL"));
   end Report_Csv;

   procedure Report_Json is
      use Ada.Text_IO;

      function Q (S : String) return String is ('"' & S & '"');
   begin
      Put_Line ("{");
      Put_Line ("  ""policy"": " & Q (if Policy = Fifo then "fifo" else "prorata") & ",");
      Put_Line ("  ""vintage_order"": " & Q (if Vintage_Order = Oldest then "oldest" else "newest") & ",");
      Put_Line ("  ""buffer_bps"": " & Img (Buffer_Bps) & ",");
      Put_Line ("  ""status"": " & Q (if Total_Allocated = Total_Demand then "FILLED" else "SHORTFALL") & ",");
      Put_Line ("  ""totals"": {""supply"": " & Img (Total_Supply) & ", ""withheld"": "
                & Img (Total_Withheld) & ", ""demand"": " & Img (Total_Demand)
                & ", ""allocated"": " & Img (Total_Allocated) & ", ""shortfall"": "
                & Img (Total_Demand - Total_Allocated) & "},");
      Put_Line ("  ""fills"": [");
      for K in 1 .. Fill_Count loop
         declare
            F : Fill_Rec renames Fills (K);
         begin
            Put_Line ("    {""order"": " & Q (Order_Name (Orders (F.Order))) & ", ""lot"": "
                      & Q (Lot_Name (Lots (F.Lot))) & ", ""vintage"": " & Img (Lots (F.Lot).Vintage)
                      & ", ""first_serial"": " & Img (F.First) & ", ""last_serial"": " & Img (F.Last)
                      & ", ""tonnes"": " & Img (F.Last - F.First + 1) & "}"
                      & (if K < Fill_Count then "," else ""));
         end;
      end loop;
      Put_Line ("  ],");
      Put_Line ("  ""orders"": [");
      for I in 1 .. Order_Count loop
         declare
            O : Order_Rec renames Orders (I);
         begin
            Put_Line ("    {""id"": " & Q (Order_Name (O)) & ", ""requested"": " & Img (O.Quantity)
                      & ", ""entitled"": " & Img (O.Entitled) & ", ""allocated"": " & Img (O.Allocated)
                      & ", ""shortfall"": " & Img (O.Quantity - O.Allocated) & "}"
                      & (if I < Order_Count then "," else ""));
         end;
      end loop;
      Put_Line ("  ],");
      Put_Line ("  ""lots"": [");
      for I in 1 .. Lot_Count loop
         declare
            L    : Lot_Rec renames Lots (I);
            Size : constant Big := L.Last - L.First + 1;
         begin
            Put_Line ("    {""id"": " & Q (Lot_Name (L)) & ", ""vintage"": " & Img (L.Vintage)
                      & ", ""size"": " & Img (Size) & ", ""withheld"": " & Img (L.Withheld)
                      & ", ""allocated"": " & Img (L.Allocated) & ", ""remaining"": "
                      & Img (Size - L.Withheld - L.Allocated) & "}"
                      & (if I < Lot_Count then "," else ""));
         end;
      end loop;
      Put_Line ("  ]");
      Put_Line ("}");
   end Report_Json;

   procedure Parse_Arguments is
      use Ada.Command_Line;
      I : Positive := 1;

      function Value_Of (Flag : String) return String is
      begin
         if I + 1 > Argument_Count then
            Fail (0, Flag & " needs a value");
         end if;
         I := I + 1;
         return Argument (I);
      end Value_Of;
   begin
      while I <= Argument_Count loop
         declare
            A : constant String := Argument (I);
         begin
            if A = "--help" or else A = "-h" then
               Usage;
               raise Help_Requested;
            elsif A = "--policy" then
               declare
                  V : constant String := Value_Of (A);
               begin
                  if V = "fifo" then
                     Policy := Fifo;
                  elsif V = "prorata" then
                     Policy := Prorata;
                  else
                     Fail (0, "--policy must be fifo or prorata");
                  end if;
               end;
            elsif A = "--vintage" then
               declare
                  V : constant String := Value_Of (A);
               begin
                  if V = "oldest" then
                     Vintage_Order := Oldest;
                  elsif V = "newest" then
                     Vintage_Order := Newest;
                  else
                     Fail (0, "--vintage must be oldest or newest");
                  end if;
               end;
            elsif A = "--buffer-bps" then
               Buffer_Bps := Parse_Number (Value_Of (A), 0, "--buffer-bps", 0, Max_Buffer_Bps);
            elsif A = "--json" then
               Want_Json := True;
            elsif A = "--fail-on-shortfall" then
               Fail_Short := True;
            elsif A'Length > 1 and then A (A'First) = '-' then
               Fail (0, "unknown option " & A);
            elsif Source_Len > 0 then
               Fail (0, "only one input file is accepted");
            elsif A'Length > Source_Name'Length then
               Fail (0, "input path is too long");
            else
               Source_Name (1 .. A'Length) := A;
               Source_Len := A'Length;
            end if;
         end;
         I := I + 1;
      end loop;
      if Source_Len = 0 then
         Fail (0, "missing input file, use - for stdin, see --help");
      end if;
   end Parse_Arguments;

   Input_File : Ada.Text_IO.File_Type;

begin
   Parse_Arguments;
   if Source_Name (1 .. Source_Len) /= "-" then
      Ada.Text_IO.Open (Input_File, Ada.Text_IO.In_File, Source_Name (1 .. Source_Len));
      Ada.Text_IO.Set_Input (Input_File);
   end if;
   Parse_Input;
   Reject_Overlapping_Lots;
   Apply_Buffer;
   Compute_Entitlements;
   Allocate;
   Self_Check;
   if Want_Json then
      Report_Json;
   else
      Report_Csv;
   end if;
   if Fail_Short and then Total_Allocated /= Total_Demand then
      Ada.Command_Line.Set_Exit_Status (Exit_Shortfall);
   end if;
exception
   when Help_Requested =>
      null;
   when E : Input_Error =>
      Ada.Text_IO.Put_Line (Ada.Text_IO.Standard_Error, "error: " & Ada.Exceptions.Exception_Message (E));
      Ada.Command_Line.Set_Exit_Status (Exit_Bad_Input);
   when E : Check_Error =>
      Ada.Text_IO.Put_Line (Ada.Text_IO.Standard_Error, "internal check failed: " & Ada.Exceptions.Exception_Message (E));
      Ada.Command_Line.Set_Exit_Status (Exit_Internal);
   when Ada.Text_IO.Name_Error | Ada.Text_IO.Use_Error =>
      Ada.Text_IO.Put_Line (Ada.Text_IO.Standard_Error, "error: cannot open input file");
      Ada.Command_Line.Set_Exit_Status (Exit_Bad_Input);
end CarbonCreditLotAllocator;
