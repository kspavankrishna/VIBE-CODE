with Ada.Characters.Handling;
with Ada.Command_Line;
with Ada.Containers.Indefinite_Hashed_Maps;
with Ada.Containers.Vectors;
with Ada.Exceptions;
with Ada.Strings.Fixed;
with Ada.Strings.Hash;
with Ada.Strings.Unbounded;
with Ada.Text_IO;

--  Simulates a Kubernetes node drain against a cluster snapshot under the
--  real eviction API rules for PodDisruptionBudgets, and reports every node
--  that cannot be drained together with the exact pod and budget that blocks
--  it. See README.md for the snapshot grammar and the model.

procedure PdbDrainPlanner is

   use Ada.Strings.Unbounded;
   use Ada.Text_IO;
   use type Ada.Containers.Count_Type;

   Input_Error : exception;
   Usage_Error : exception;

   subtype Amount is Long_Long_Integer;

   Max_Line_Length : constant := 16_384;
   Max_Records     : constant := 1_000_000;
   Max_Steps_Cap   : constant := 10_000_000;
   Max_Parallel_Cap : constant := 10_000;
   Quantity_Limit  : constant Amount := 10 ** 17;
   Never           : constant Natural := Natural'Last;

   Exit_Complete : constant := 0;
   Exit_Blocked  : constant := 1;
   Exit_Usage    : constant := 2;

   function U (Text : String) return Unbounded_String
     renames To_Unbounded_String;
   function Str (Value : Unbounded_String) return String renames To_String;

   function Img (X : Amount) return String is
     (Ada.Strings.Fixed.Trim (Amount'Image (X), Ada.Strings.Left));

   function NImg (X : Natural) return String is (Img (Amount (X)));

   ---------------------------------------------------------------------------
   --  Containers
   ---------------------------------------------------------------------------

   package Index_Vectors is new Ada.Containers.Vectors (Positive, Positive);
   package String_Vectors is new Ada.Containers.Vectors
     (Positive, Unbounded_String);
   package Name_Maps is new Ada.Containers.Indefinite_Hashed_Maps
     (Key_Type        => String,
      Element_Type    => Positive,
      Hash            => Ada.Strings.Hash,
      Equivalent_Keys => "=");

   type Label is record
      Key   : Unbounded_String;
      Value : Unbounded_String;
   end record;
   package Label_Vectors is new Ada.Containers.Vectors (Positive, Label);

   type Selector_Op is (Op_Equals, Op_In, Op_Not_In, Op_Exists, Op_Not_Exists);

   type Term is record
      Key    : Unbounded_String;
      Op     : Selector_Op := Op_Equals;
      Values : String_Vectors.Vector;
   end record;
   package Term_Vectors is new Ada.Containers.Vectors (Positive, Term);

   ---------------------------------------------------------------------------
   --  Snapshot model
   ---------------------------------------------------------------------------

   type Node_Role is (Keep, Target);
   type Node_State is (Active, Draining, Drained, Stuck);

   type Node_Rec is record
      Name        : Unbounded_String;
      Pool        : Unbounded_String;
      Cpu         : Amount := 0;
      Mem         : Amount := 0;
      Slots       : Amount := 0;
      Role        : Node_Role := Keep;
      State       : Node_State := Active;
      Cordoned    : Boolean := False;
      Used_Cpu    : Amount := 0;
      Used_Mem    : Amount := 0;
      Used_Slots  : Amount := 0;
      Movable     : Natural := 0;
      Start_Step  : Natural := Never;
      End_Step    : Natural := Never;
      Order       : Natural := 0;
      Evicted     : Natural := 0;
      Landed      : Natural := 0;
   end record;
   package Node_Vectors is new Ada.Containers.Vectors (Positive, Node_Rec);

   type Workload_Rec is record
      Name          : Unbounded_String;
      Replicas      : Amount := 0;
      Ready_Seconds : Amount := 0;
      Ready_Steps   : Natural := 1;
      Pod_Count     : Natural := 0;
      Line          : Positive := 1;
   end record;
   package Workload_Vectors is new Ada.Containers.Vectors
     (Positive, Workload_Rec);

   type Budget_Kind is (Min_Available, Max_Unavailable);
   type Eviction_Policy is (If_Healthy_Budget, Always_Allow);

   type Budget_Rec is record
      Name        : Unbounded_String;
      Namespace   : Unbounded_String;
      Selector    : Term_Vectors.Vector;
      Match_All   : Boolean := False;
      Match_None  : Boolean := False;
      Kind        : Budget_Kind := Min_Available;
      Value       : Amount := 0;
      Percent     : Boolean := False;
      Policy      : Eviction_Policy := If_Healthy_Budget;
      Members     : Natural := 0;
      Naked       : Natural := 0;
      Healthy     : Natural := 0;
      Scale       : Amount := 0;
      Min_Healthy : Natural := Never;
      Max_Down    : Natural := 0;
      Touches_Target : Boolean := False;
      Line        : Positive := 1;
   end record;
   package Budget_Vectors is new Ada.Containers.Vectors (Positive, Budget_Rec);

   type Pod_Status is (Ready, Starting, Pending, Gone);

   type Pod_Rec is record
      Name          : Unbounded_String;
      Namespace     : Unbounded_String;
      Node_Name     : Unbounded_String;
      Workload_Name : Unbounded_String;
      Labels        : Label_Vectors.Vector;
      Node          : Natural := 0;
      Start_Node    : Natural := 0;
      Workload      : Natural := 0;
      Cpu           : Amount := 0;
      Mem           : Amount := 0;
      Pool          : Unbounded_String;
      Daemon        : Boolean := False;
      Mirror        : Boolean := False;
      Empty_Dir     : Boolean := False;
      Broken        : Boolean := False;
      Status        : Pod_Status := Ready;
      Ready_At      : Natural := Never;
      Evictions     : Natural := 0;
      Covering      : Index_Vectors.Vector;
      Line          : Positive := 1;
   end record;
   package Pod_Vectors is new Ada.Containers.Vectors (Positive, Pod_Rec);

   Nodes     : Node_Vectors.Vector;
   Workloads : Workload_Vectors.Vector;
   Budgets   : Budget_Vectors.Vector;
   Pods      : Pod_Vectors.Vector;

   Node_Index     : Name_Maps.Map;
   Workload_Index : Name_Maps.Map;
   Budget_Index   : Name_Maps.Map;
   Pod_Index      : Name_Maps.Map;

   ---------------------------------------------------------------------------
   --  Settings and results
   ---------------------------------------------------------------------------

   type Settings_Rec is record
      Tick_Seconds     : Amount := 5;
      Max_Parallel     : Natural := 1;
      Max_Steps        : Natural := 100_000;
      Cordon_All       : Boolean := False;
      Force            : Boolean := False;
      Delete_Empty_Dir : Boolean := False;
      Json             : Boolean := False;
      Strict           : Boolean := False;
      Path             : Unbounded_String := U ("-");
   end record;
   Settings : Settings_Rec;

   type Severity is (Error, Warning);

   type Finding is record
      Level   : Severity := Warning;
      Code    : Unbounded_String;
      Subject : Unbounded_String;
      Message : Unbounded_String;
   end record;
   package Finding_Vectors is new Ada.Containers.Vectors (Positive, Finding);
   Findings : Finding_Vectors.Vector;

   type Blocker is record
      Node    : Positive := 1;
      Pod     : Positive := 1;
      Code    : Unbounded_String;
      Message : Unbounded_String;
   end record;
   package Blocker_Vectors is new Ada.Containers.Vectors (Positive, Blocker);
   Blockers : Blocker_Vectors.Vector;

   type Verdict is
     (Evictable, Skipped, Blocked_Unmanaged, Blocked_Empty_Dir,
      Blocked_Multiple_Budgets, Blocked_Budget_Error, Blocked_Budget);

   Final_Step      : Natural := 0;
   Settled_Step    : Natural := 0;
   Timed_Out       : Boolean := False;
   Total_Evictions : Natural := 0;
   Repeat_Evictions : Natural := 0;
   Pending_Queue   : Index_Vectors.Vector;
   Sorted_Pods     : Index_Vectors.Vector;

   procedure Add_Finding (Level : Severity; Code, Subject, Message : String) is
   begin
      Findings.Append
        ((Level => Level, Code => U (Code), Subject => U (Subject),
          Message => U (Message)));
   end Add_Finding;

   ---------------------------------------------------------------------------
   --  Lexical helpers
   ---------------------------------------------------------------------------

   procedure Fail (Line_No : Natural; Message : String);
   pragma No_Return (Fail);

   procedure Fail (Line_No : Natural; Message : String) is
   begin
      raise Input_Error with "line " & NImg (Line_No) & ": " & Message;
   end Fail;

   procedure Split
     (Line : String; Sep : Character; Parts : out String_Vectors.Vector)
   is
      Start : Positive := Line'First;
   begin
      Parts.Clear;
      for I in Line'Range loop
         if Line (I) = Sep then
            Parts.Append (U (Line (Start .. I - 1)));
            Start := I + 1;
         end if;
      end loop;
      Parts.Append (U (Line (Start .. Line'Last)));
   end Split;

   procedure Tokenize (Line : String; Tokens : out String_Vectors.Vector) is
      I : Natural := Line'First;
      function Blank (C : Character) return Boolean is
        (C = ' ' or else C = ASCII.HT or else C = ASCII.CR);
   begin
      Tokens.Clear;
      loop
         while I <= Line'Last and then Blank (Line (I)) loop
            I := I + 1;
         end loop;
         exit when I > Line'Last or else Line (I) = '#';
         declare
            Start : constant Positive := I;
         begin
            while I <= Line'Last and then not Blank (Line (I)) loop
               I := I + 1;
            end loop;
            Tokens.Append (U (Line (Start .. I - 1)));
         end;
      end loop;
   end Tokenize;

   function Mul (A, B : Amount; Line_No : Natural; What : String) return Amount
   is
   begin
      if A /= 0 and then B > Quantity_Limit / A then
         Fail (Line_No, What & " is too large");
      end if;
      return A * B;
   end Mul;

   function Parse_Natural
     (Text : String; Line_No : Natural; What : String) return Amount
   is
      Result : Amount := 0;
   begin
      if Text'Length = 0 or else Text'Length > 17 then
         Fail (Line_No, What & " must be a whole number, got '" & Text & "'");
      end if;
      for C of Text loop
         if C not in '0' .. '9' then
            Fail (Line_No, What & " must be a whole number, got '" & Text & "'");
         end if;
         Result := Result * 10 + Amount (Character'Pos (C) - Character'Pos ('0'));
      end loop;
      return Result;
   end Parse_Natural;

   --  Kubernetes resource quantity: digits, optional fraction, optional
   --  decimal or binary suffix. Unit is the multiplier into the internal
   --  unit (1000 for CPU millicores, 1 for memory bytes). Rounds up, like
   --  the scheduler does when it converts requests.
   function Parse_Quantity
     (Text : String; Unit : Amount; Line_No : Natural; What : String)
      return Amount
   is
      I           : Natural := Text'First;
      Mantissa    : Amount := 0;
      Int_Digits  : Natural := 0;
      Frac_Digits : Natural := 0;
      Num         : Amount := 1;
      Den         : Amount := 1;
   begin
      while I <= Text'Last and then Text (I) in '0' .. '9' loop
         Mantissa := Mul (Mantissa, 10, Line_No, What)
           + Amount (Character'Pos (Text (I)) - Character'Pos ('0'));
         Int_Digits := Int_Digits + 1;
         I := I + 1;
      end loop;
      if I <= Text'Last and then Text (I) = '.' then
         I := I + 1;
         while I <= Text'Last and then Text (I) in '0' .. '9' loop
            Mantissa := Mul (Mantissa, 10, Line_No, What)
              + Amount (Character'Pos (Text (I)) - Character'Pos ('0'));
            Frac_Digits := Frac_Digits + 1;
            I := I + 1;
         end loop;
      end if;
      if Int_Digits + Frac_Digits = 0 or else Frac_Digits > 9 then
         Fail (Line_No, What & " is not a valid quantity: '" & Text & "'");
      end if;
      declare
         Suffix : constant String := Text (I .. Text'Last);
      begin
         if Suffix = "" then
            null;
         elsif Suffix = "m" then
            Den := 1000;
         elsif Suffix = "k" then
            Num := 10 ** 3;
         elsif Suffix = "M" then
            Num := 10 ** 6;
         elsif Suffix = "G" then
            Num := 10 ** 9;
         elsif Suffix = "T" then
            Num := 10 ** 12;
         elsif Suffix = "P" then
            Num := 10 ** 15;
         elsif Suffix = "Ki" then
            Num := 2 ** 10;
         elsif Suffix = "Mi" then
            Num := 2 ** 20;
         elsif Suffix = "Gi" then
            Num := 2 ** 30;
         elsif Suffix = "Ti" then
            Num := 2 ** 40;
         elsif Suffix = "Pi" then
            Num := 2 ** 50;
         else
            Fail (Line_No, What & " has an unsupported suffix: '" & Text & "'");
         end if;
      end;
      Den := Den * 10 ** Frac_Digits;
      declare
         Top : constant Amount :=
           Mul (Mul (Mantissa, Num, Line_No, What), Unit, Line_No, What);
      begin
         return (Top + Den - 1) / Den;
      end;
   end Parse_Quantity;

   --  A request may be written as a sum, one term per container, and as a
   --  maximum over alternatives separated by ^ so init containers can be
   --  expressed: 100m+250m^500m is max (350m, 500m), the effective request
   --  the scheduler uses.
   function Parse_Request
     (Text : String; Unit : Amount; Line_No : Natural; What : String)
      return Amount
   is
      Groups : String_Vectors.Vector;
      Parts  : String_Vectors.Vector;
      Best   : Amount := 0;
   begin
      Split (Text, '^', Groups);
      for Group of Groups loop
         declare
            Total : Amount := 0;
         begin
            Split (Str (Group), '+', Parts);
            for Part of Parts loop
               Total := Total + Parse_Quantity (Str (Part), Unit, Line_No, What);
               if Total > Quantity_Limit then
                  Fail (Line_No, What & " is too large");
               end if;
            end loop;
            Best := Amount'Max (Best, Total);
         end;
      end loop;
      return Best;
   end Parse_Request;

   function Is_Key_Char (C : Character) return Boolean is
     (Ada.Characters.Handling.Is_Alphanumeric (C)
      or else C = '.' or else C = '_' or else C = '-' or else C = '/');

   function Is_Value_Char (C : Character) return Boolean is
     (Ada.Characters.Handling.Is_Alphanumeric (C)
      or else C = '.' or else C = '_' or else C = '-');

   procedure Check_Key (Key : String; Line_No : Natural) is
   begin
      if Key'Length = 0 then
         Fail (Line_No, "empty label key");
      end if;
      for C of Key loop
         if not Is_Key_Char (C) then
            Fail (Line_No, "invalid character in label key '" & Key & "'");
         end if;
      end loop;
   end Check_Key;

   procedure Check_Value (Value : String; Line_No : Natural) is
   begin
      for C of Value loop
         if not Is_Value_Char (C) then
            Fail (Line_No, "invalid character in label value '" & Value & "'");
         end if;
      end loop;
   end Check_Value;

   function Namespace_Of (Name : String; Line_No : Natural) return String is
      Slash : constant Natural := Ada.Strings.Fixed.Index (Name, "/");
   begin
      if Slash <= Name'First or else Slash = Name'Last
        or else Ada.Strings.Fixed.Index (Name (Slash + 1 .. Name'Last), "/") /= 0
      then
         Fail (Line_No, "name must look like namespace/name, got '" & Name & "'");
      end if;
      return Name (Name'First .. Slash - 1);
   end Namespace_Of;

   function Parse_Labels
     (Text : String; Line_No : Natural) return Label_Vectors.Vector
   is
      Result : Label_Vectors.Vector;
      Parts  : String_Vectors.Vector;
   begin
      if Text = "-" then
         return Result;
      end if;
      Split (Text, ',', Parts);
      for Part of Parts loop
         declare
            P  : constant String := Str (Part);
            Eq : constant Natural := Ada.Strings.Fixed.Index (P, "=");
         begin
            if Eq = 0 then
               Fail (Line_No, "label '" & P & "' must be key=value");
            end if;
            declare
               Key   : constant String := P (P'First .. Eq - 1);
               Value : constant String := P (Eq + 1 .. P'Last);
            begin
               Check_Key (Key, Line_No);
               Check_Value (Value, Line_No);
               for Existing of Result loop
                  if Str (Existing.Key) = Key then
                     Fail (Line_No, "duplicate label key '" & Key & "'");
                  end if;
               end loop;
               Result.Append ((Key => U (Key), Value => U (Value)));
            end;
         end;
      end loop;
      return Result;
   end Parse_Labels;

   procedure Parse_Selector
     (Text       : String;
      Line_No    : Natural;
      Terms      : out Term_Vectors.Vector;
      Match_All  : out Boolean;
      Match_None : out Boolean)
   is
      Parts : String_Vectors.Vector;
   begin
      Terms.Clear;
      Match_All := Text = "*";
      Match_None := Text = "!";
      if Match_All or else Match_None then
         return;
      end if;
      Split (Text, ',', Parts);
      for Part of Parts loop
         declare
            P  : constant String := Str (Part);
            At_Sign : constant Natural := Ada.Strings.Fixed.Index (P, "@");
            Eq : constant Natural := Ada.Strings.Fixed.Index (P, "=");
            T  : Term;
         begin
            if At_Sign > 0 then
               declare
                  Key   : constant String := P (P'First .. At_Sign - 1);
                  Rest  : constant String := P (At_Sign + 1 .. P'Last);
                  Colon : constant Natural := Ada.Strings.Fixed.Index (Rest, ":");
                  Op    : constant String :=
                    (if Colon = 0 then Rest else Rest (Rest'First .. Colon - 1));
                  Vals  : String_Vectors.Vector;
               begin
                  Check_Key (Key, Line_No);
                  T.Key := U (Key);
                  if Op = "in" then
                     T.Op := Op_In;
                  elsif Op = "notin" then
                     T.Op := Op_Not_In;
                  elsif Op = "exists" then
                     T.Op := Op_Exists;
                  elsif Op = "notexists" then
                     T.Op := Op_Not_Exists;
                  else
                     Fail (Line_No, "unknown selector operator '" & Op & "'");
                  end if;
                  if T.Op in Op_In | Op_Not_In then
                     if Colon = 0 or else Colon = Rest'Last then
                        Fail (Line_No, "operator " & Op & " needs values: key@" & Op & ":a|b");
                     end if;
                     Split (Rest (Colon + 1 .. Rest'Last), '|', Vals);
                     for V of Vals loop
                        Check_Value (Str (V), Line_No);
                     end loop;
                     T.Values := Vals;
                  elsif Colon /= 0 then
                     Fail (Line_No, "operator " & Op & " takes no values");
                  end if;
               end;
            elsif Eq > 0 then
               Check_Key (P (P'First .. Eq - 1), Line_No);
               Check_Value (P (Eq + 1 .. P'Last), Line_No);
               T.Key := U (P (P'First .. Eq - 1));
               T.Op := Op_Equals;
               T.Values.Append (U (P (Eq + 1 .. P'Last)));
            else
               Fail (Line_No, "selector term '" & P & "' is neither key=value nor key@op");
            end if;
            Terms.Append (T);
         end;
      end loop;
   end Parse_Selector;

   function Selects (B : Budget_Rec; P : Pod_Rec) return Boolean is
   begin
      if B.Namespace /= P.Namespace or else B.Match_None then
         return False;
      end if;
      if B.Match_All then
         return True;
      end if;
      for T of B.Selector loop
         declare
            Found : Boolean := False;
            Value : Unbounded_String;
            Hit   : Boolean := False;
         begin
            for L of P.Labels loop
               if L.Key = T.Key then
                  Found := True;
                  Value := L.Value;
                  exit;
               end if;
            end loop;
            if Found then
               for V of T.Values loop
                  if V = Value then
                     Hit := True;
                     exit;
                  end if;
               end loop;
            end if;
            case T.Op is
               when Op_Equals | Op_In =>
                  if not (Found and then Hit) then
                     return False;
                  end if;
               when Op_Not_In =>
                  if Found and then Hit then
                     return False;
                  end if;
               when Op_Exists =>
                  if not Found then
                     return False;
                  end if;
               when Op_Not_Exists =>
                  if Found then
                     return False;
                  end if;
            end case;
         end;
      end loop;
      return True;
   end Selects;

   ---------------------------------------------------------------------------
   --  Loading
   ---------------------------------------------------------------------------

   procedure Register
     (Map : in out Name_Maps.Map; Name : String; Index : Positive;
      Kind : String; Line_No : Natural) is
   begin
      if Map.Contains (Name) then
         Fail (Line_No, "duplicate " & Kind & " '" & Name & "'");
      end if;
      Map.Insert (Name, Index);
   end Register;

   procedure Parse_Line (Tokens : String_Vectors.Vector; Line_No : Positive) is
      Kind : constant String := Str (Tokens (1));

      function T (I : Positive) return String is (Str (Tokens (I)));

      procedure Expect (Count : Positive; Grammar : String) is
      begin
         if Natural (Tokens.Length) /= Count then
            Fail (Line_No, Kind & " needs " & NImg (Count - 1)
                  & " fields: " & Grammar);
         end if;
      end Expect;
   begin
      if Kind = "node" then
         Expect (8, "node NAME POOL CPU MEMORY PODS keep|drain schedulable|cordoned");
         declare
            N : Node_Rec;
         begin
            N.Name := U (T (2));
            N.Pool := U (T (3));
            N.Cpu := Parse_Quantity (T (4), 1000, Line_No, "node cpu");
            N.Mem := Parse_Quantity (T (5), 1, Line_No, "node memory");
            N.Slots := Parse_Natural (T (6), Line_No, "node pod capacity");
            if T (7) = "drain" then
               N.Role := Target;
            elsif T (7) /= "keep" then
               Fail (Line_No, "node role must be keep or drain");
            end if;
            if T (8) = "cordoned" then
               N.Cordoned := True;
            elsif T (8) /= "schedulable" then
               Fail (Line_No, "node state must be schedulable or cordoned");
            end if;
            Nodes.Append (N);
            Register (Node_Index, T (2), Nodes.Last_Index, "node", Line_No);
         end;

      elsif Kind = "workload" then
         Expect (4, "workload NAMESPACE/NAME REPLICAS READY_SECONDS");
         declare
            W : Workload_Rec;
         begin
            W.Name := U (T (2));
            if Namespace_Of (T (2), Line_No)'Length = 0 then
               Fail (Line_No, "workload needs a namespace");
            end if;
            W.Replicas := Parse_Natural (T (3), Line_No, "replicas");
            W.Ready_Seconds := Parse_Natural (T (4), Line_No, "ready seconds");
            W.Line := Line_No;
            Workloads.Append (W);
            Register (Workload_Index, T (2), Workloads.Last_Index, "workload", Line_No);
         end;

      elsif Kind = "pdb" then
         Expect (6, "pdb NAMESPACE/NAME minAvailable|maxUnavailable N|N% "
                 & "IfHealthyBudget|AlwaysAllow SELECTOR");
         declare
            B   : Budget_Rec;
            Raw : constant String := T (4);
         begin
            B.Name := U (T (2));
            B.Namespace := U (Namespace_Of (T (2), Line_No));
            if T (3) = "minAvailable" then
               B.Kind := Min_Available;
            elsif T (3) = "maxUnavailable" then
               B.Kind := Max_Unavailable;
            else
               Fail (Line_No, "budget kind must be minAvailable or maxUnavailable");
            end if;
            if Raw'Length > 0 and then Raw (Raw'Last) = '%' then
               B.Percent := True;
               B.Value := Parse_Natural (Raw (Raw'First .. Raw'Last - 1), Line_No, "budget percent");
               if B.Value > 100 then
                  Fail (Line_No, "budget percent must be between 0% and 100%");
               end if;
            else
               B.Value := Parse_Natural (Raw, Line_No, "budget value");
            end if;
            if T (5) = "IfHealthyBudget" then
               B.Policy := If_Healthy_Budget;
            elsif T (5) = "AlwaysAllow" then
               B.Policy := Always_Allow;
            else
               Fail (Line_No, "unhealthy pod eviction policy must be IfHealthyBudget or AlwaysAllow");
            end if;
            Parse_Selector (T (6), Line_No, B.Selector, B.Match_All, B.Match_None);
            B.Line := Line_No;
            Budgets.Append (B);
            Register (Budget_Index, T (2), Budgets.Last_Index, "pdb", Line_No);
         end;

      elsif Kind = "pod" then
         Expect (10, "pod NAMESPACE/NAME NODE WORKLOAD|- CPU MEMORY ready|unready "
                 & "POOL|- LABELS|- FLAGS|-");
         declare
            P     : Pod_Rec;
            Flags : String_Vectors.Vector;
         begin
            P.Name := U (T (2));
            P.Namespace := U (Namespace_Of (T (2), Line_No));
            P.Node_Name := U (T (3));
            P.Workload_Name := (if T (4) = "-" then Null_Unbounded_String else U (T (4)));
            P.Cpu := Parse_Request (T (5), 1000, Line_No, "pod cpu");
            P.Mem := Parse_Request (T (6), 1, Line_No, "pod memory");
            if T (7) = "unready" then
               P.Status := Starting;
               P.Broken := True;
            elsif T (7) /= "ready" then
               Fail (Line_No, "pod readiness must be ready or unready");
            end if;
            P.Pool := (if T (8) = "-" then Null_Unbounded_String else U (T (8)));
            P.Labels := Parse_Labels (T (9), Line_No);
            if T (10) /= "-" then
               Split (T (10), ',', Flags);
               for F of Flags loop
                  if Str (F) = "ds" then
                     P.Daemon := True;
                  elsif Str (F) = "mirror" then
                     P.Mirror := True;
                  elsif Str (F) = "emptydir" then
                     P.Empty_Dir := True;
                  else
                     Fail (Line_No, "unknown pod flag '" & Str (F)
                           & "', expected ds, mirror or emptydir");
                  end if;
               end loop;
            end if;
            P.Line := Line_No;
            Pods.Append (P);
            Register (Pod_Index, T (2), Pods.Last_Index, "pod", Line_No);
         end;

      else
         Fail (Line_No, "unknown record type '" & Kind
               & "', expected node, workload, pdb or pod");
      end if;
   end Parse_Line;

   procedure Load_From (File : File_Type) is
      Line_No : Natural := 0;
      Tokens  : String_Vectors.Vector;
   begin
      while not End_Of_File (File) loop
         declare
            Line : constant String := Get_Line (File);
         begin
            Line_No := Line_No + 1;
            if Line'Length > Max_Line_Length then
               Fail (Line_No, "line longer than" & Integer'Image (Max_Line_Length)
                     & " characters");
            end if;
            Tokenize (Line, Tokens);
            if not Tokens.Is_Empty then
               if Natural (Nodes.Length + Workloads.Length + Budgets.Length
                           + Pods.Length) >= Max_Records
               then
                  Fail (Line_No, "more than" & Integer'Image (Max_Records) & " records");
               end if;
               Parse_Line (Tokens, Line_No);
            end if;
         end;
      end loop;
   end Load_From;

   procedure Load is
   begin
      if Str (Settings.Path) = "-" then
         Load_From (Standard_Input);
      else
         declare
            File : File_Type;
         begin
            Open (File, In_File, Str (Settings.Path));
            Load_From (File);
            Close (File);
         exception
            when Name_Error | Use_Error =>
               raise Input_Error with "cannot open '" & Str (Settings.Path) & "'";
            when others =>
               if Is_Open (File) then
                  Close (File);
               end if;
               raise;
         end;
      end if;
   end Load;

   ---------------------------------------------------------------------------
   --  Budget arithmetic, mirroring the disruption controller
   ---------------------------------------------------------------------------

   function Budget_Error (Q : Positive) return Boolean is
     ((Budgets (Q).Kind = Max_Unavailable or else Budgets (Q).Percent)
      and then Budgets (Q).Naked > 0);

   function Expected (Q : Positive) return Amount is
     (if Budgets (Q).Kind = Min_Available and then not Budgets (Q).Percent
      then Amount (Budgets (Q).Members)
      else Budgets (Q).Scale);

   function Ceil_Percent (Value, Total : Amount) return Amount is
     ((Value * Total + 99) / 100);

   function Desired (Q : Positive) return Amount is
      B : Budget_Rec renames Budgets (Q);
      E : constant Amount := Expected (Q);
      D : Amount;
   begin
      if B.Kind = Min_Available then
         D := (if B.Percent then Ceil_Percent (B.Value, E) else B.Value);
      else
         D := E - (if B.Percent then Ceil_Percent (B.Value, E) else B.Value);
      end if;
      return Amount'Max (0, D);
   end Desired;

   function Allowed (Q : Positive) return Amount is
     (if Budget_Error (Q) then 0
      else Amount'Max (0, Amount (Budgets (Q).Healthy) - Desired (Q)));

   function Ever_Allows (Q : Positive) return Boolean is
     (not Budget_Error (Q)
      and then Amount (Budgets (Q).Members) - Desired (Q) > 0);

   procedure Track (Q : Positive) is
      B : Budget_Rec renames Budgets (Q);
   begin
      B.Min_Healthy := Natural'Min (B.Min_Healthy, B.Healthy);
      if B.Members > B.Healthy then
         B.Max_Down := Natural'Max (B.Max_Down, B.Members - B.Healthy);
      end if;
   end Track;

   procedure Set_Status (P : Positive; New_Status : Pod_Status) is
      Old_Status : constant Pod_Status := Pods (P).Status;
      Cover      : constant Index_Vectors.Vector := Pods (P).Covering;
      Naked      : constant Boolean := Pods (P).Workload = 0;
   begin
      if Old_Status = New_Status then
         return;
      end if;
      for Q of Cover loop
         if Old_Status = Ready then
            Budgets (Q).Healthy := Budgets (Q).Healthy - 1;
         elsif New_Status = Ready then
            Budgets (Q).Healthy := Budgets (Q).Healthy + 1;
         end if;
         if New_Status = Gone then
            Budgets (Q).Members := Budgets (Q).Members - 1;
            if Naked then
               Budgets (Q).Naked := Budgets (Q).Naked - 1;
            end if;
         end if;
         Track (Q);
      end loop;
      Pods (P).Status := New_Status;
   end Set_Status;

   ---------------------------------------------------------------------------
   --  Placement
   ---------------------------------------------------------------------------

   procedure Assign (P : Positive; N : Positive) is
   begin
      Nodes (N).Used_Cpu := Nodes (N).Used_Cpu + Pods (P).Cpu;
      Nodes (N).Used_Mem := Nodes (N).Used_Mem + Pods (P).Mem;
      Nodes (N).Used_Slots := Nodes (N).Used_Slots + 1;
      if not Pods (P).Daemon and then not Pods (P).Mirror then
         Nodes (N).Movable := Nodes (N).Movable + 1;
      end if;
      Pods (P).Node := N;
   end Assign;

   procedure Release (P : Positive) is
      N : constant Positive := Pods (P).Node;
   begin
      Nodes (N).Used_Cpu := Nodes (N).Used_Cpu - Pods (P).Cpu;
      Nodes (N).Used_Mem := Nodes (N).Used_Mem - Pods (P).Mem;
      Nodes (N).Used_Slots := Nodes (N).Used_Slots - 1;
      if not Pods (P).Daemon and then not Pods (P).Mirror then
         Nodes (N).Movable := Nodes (N).Movable - 1;
      end if;
      Pods (P).Node := 0;
   end Release;

   function Fits (N, P : Positive) return Boolean is
      Node : Node_Rec renames Nodes (N);
   begin
      return Node.State = Active
        and then not Node.Cordoned
        and then (Length (Pods (P).Pool) = 0 or else Pods (P).Pool = Node.Pool)
        and then Node.Used_Cpu + Pods (P).Cpu <= Node.Cpu
        and then Node.Used_Mem + Pods (P).Mem <= Node.Mem
        and then Node.Used_Slots + 1 <= Node.Slots;
   end Fits;

   --  Default kube-scheduler scoring is LeastAllocated: prefer the node that
   --  keeps the most free CPU and memory, averaged, after placement.
   function Score (N, P : Positive) return Long_Float is
      Node : Node_Rec renames Nodes (N);
      function Free (Cap, Used, Req : Amount) return Long_Float is
        (if Cap <= 0 then 0.0
         else Long_Float (Cap - Used - Req) / Long_Float (Cap));
   begin
      return (Free (Node.Cpu, Node.Used_Cpu, Pods (P).Cpu)
              + Free (Node.Mem, Node.Used_Mem, Pods (P).Mem)) / 2.0;
   end Score;

   function Place (P : Positive; Step : Natural) return Boolean is
      Best       : Natural := 0;
      Best_Score : Long_Float := Long_Float'First;
   begin
      for N in Nodes.First_Index .. Nodes.Last_Index loop
         if Fits (N, P) then
            declare
               S : constant Long_Float := Score (N, P);
            begin
               if Best = 0 or else S > Best_Score
                 or else (S = Best_Score and then Nodes (N).Name < Nodes (Best).Name)
               then
                  Best := N;
                  Best_Score := S;
               end if;
            end;
         end if;
      end loop;
      if Best = 0 then
         return False;
      end if;
      Assign (P, Best);
      Nodes (Best).Landed := Nodes (Best).Landed + 1;
      Set_Status (P, Starting);
      if Pods (P).Broken then
         Pods (P).Ready_At := Never;
      else
         Pods (P).Ready_At := Step + Workloads (Pods (P).Workload).Ready_Steps;
      end if;
      return True;
   end Place;

   ---------------------------------------------------------------------------
   --  Eviction API rules
   ---------------------------------------------------------------------------

   function Judge (P : Positive) return Verdict is
      Q : Positive;
   begin
      if Pods (P).Daemon or else Pods (P).Mirror then
         return Skipped;
      end if;
      if Pods (P).Workload = 0 and then not Settings.Force then
         return Blocked_Unmanaged;
      end if;
      if Pods (P).Empty_Dir and then not Settings.Delete_Empty_Dir then
         return Blocked_Empty_Dir;
      end if;
      if Pods (P).Covering.Is_Empty then
         return Evictable;
      end if;
      if Pods (P).Covering.Length > 1 then
         return Blocked_Multiple_Budgets;
      end if;
      Q := Pods (P).Covering.First_Element;
      if Pods (P).Status /= Ready then
         if Budgets (Q).Policy = Always_Allow then
            return Evictable;
         end if;
         if not Budget_Error (Q)
           and then Desired (Q) > 0
           and then Amount (Budgets (Q).Healthy) >= Desired (Q)
         then
            return Evictable;
         end if;
      end if;
      if Budget_Error (Q) then
         return Blocked_Budget_Error;
      end if;
      return (if Allowed (Q) > 0 then Evictable else Blocked_Budget);
   end Judge;

   procedure Evict (P : Positive) is
      N : constant Positive := Pods (P).Node;
   begin
      Release (P);
      Nodes (N).Evicted := Nodes (N).Evicted + 1;
      Pods (P).Evictions := Pods (P).Evictions + 1;
      Total_Evictions := Total_Evictions + 1;
      if Pods (P).Evictions > 1 then
         Repeat_Evictions := Repeat_Evictions + 1;
      end if;
      Pods (P).Ready_At := Never;
      if Pods (P).Workload = 0 then
         Set_Status (P, Gone);
      else
         Set_Status (P, Pending);
         Pending_Queue.Append (P);
      end if;
   end Evict;

   ---------------------------------------------------------------------------
   --  Resolution and static findings
   ---------------------------------------------------------------------------

   function Pod_Less (A, B : Positive) return Boolean is
     (Pods (A).Name < Pods (B).Name);
   package Pod_Sorting is new Index_Vectors.Generic_Sorting ("<" => Pod_Less);

   procedure Resolve is
   begin
      for P in Pods.First_Index .. Pods.Last_Index loop
         declare
            Node_Name : constant String := Str (Pods (P).Node_Name);
            Wl_Name   : constant String := Str (Pods (P).Workload_Name);
         begin
            if not Node_Index.Contains (Node_Name) then
               Fail (Pods (P).Line, "pod " & Str (Pods (P).Name)
                     & " is on unknown node '" & Node_Name & "'");
            end if;
            if Wl_Name /= "" then
               if not Workload_Index.Contains (Wl_Name) then
                  Fail (Pods (P).Line, "pod " & Str (Pods (P).Name)
                        & " references unknown workload '" & Wl_Name & "'");
               end if;
               Pods (P).Workload := Workload_Index.Element (Wl_Name);
               if Namespace_Of (Wl_Name, Pods (P).Line) /= Str (Pods (P).Namespace) then
                  Fail (Pods (P).Line, "pod " & Str (Pods (P).Name)
                        & " and its workload are in different namespaces");
               end if;
               Workloads (Pods (P).Workload).Pod_Count :=
                 Workloads (Pods (P).Workload).Pod_Count + 1;
            end if;
            if (Pods (P).Daemon or else Pods (P).Mirror) and then Pods (P).Workload /= 0 then
               Fail (Pods (P).Line, "a ds or mirror pod must use - as its workload");
            end if;
            Pods (P).Start_Node := Node_Index.Element (Node_Name);
            Assign (P, Pods (P).Start_Node);
         end;
      end loop;

      for W in Workloads.First_Index .. Workloads.Last_Index loop
         Workloads (W).Ready_Steps := Natural'Max
           (1, Natural ((Workloads (W).Ready_Seconds + Settings.Tick_Seconds - 1)
                        / Settings.Tick_Seconds));
      end loop;

      for Q in Budgets.First_Index .. Budgets.Last_Index loop
         declare
            Seen : Index_Vectors.Vector;
         begin
            for P in Pods.First_Index .. Pods.Last_Index loop
               if Selects (Budgets (Q), Pods (P)) then
                  Pods (P).Covering.Append (Q);
                  Budgets (Q).Members := Budgets (Q).Members + 1;
                  if Pods (P).Status = Ready then
                     Budgets (Q).Healthy := Budgets (Q).Healthy + 1;
                  end if;
                  if Pods (P).Workload = 0 then
                     Budgets (Q).Naked := Budgets (Q).Naked + 1;
                  elsif not Seen.Contains (Pods (P).Workload) then
                     Seen.Append (Pods (P).Workload);
                     Budgets (Q).Scale :=
                       Budgets (Q).Scale + Workloads (Pods (P).Workload).Replicas;
                  end if;
                  if Nodes (Pods (P).Node).Role = Target then
                     Budgets (Q).Touches_Target := True;
                  end if;
               end if;
            end loop;
            Track (Q);
         end;
      end loop;

      for P in Pods.First_Index .. Pods.Last_Index loop
         Sorted_Pods.Append (P);
      end loop;
      Pod_Sorting.Sort (Sorted_Pods);
   end Resolve;

   function Budget_Numbers (Q : Positive) return String is
     ("healthy " & NImg (Budgets (Q).Healthy)
      & ", desired " & Img (Desired (Q))
      & ", expected " & Img (Expected (Q)));

   procedure Lint is
      Warned_Single : Index_Vectors.Vector;

      type Pool_Total is record
         Pool                    : Unbounded_String;
         Free_Cpu, Free_Mem      : Amount := 0;
         Free_Slots              : Amount := 0;
         Need_Cpu, Need_Mem      : Amount := 0;
         Need_Slots              : Amount := 0;
      end record;
      package Pool_Vectors is new Ada.Containers.Vectors (Positive, Pool_Total);
      Pools : Pool_Vectors.Vector;
      All_Pools : Pool_Total;

      function Pool_Slot (Name : Unbounded_String) return Positive is
      begin
         for I in Pools.First_Index .. Pools.Last_Index loop
            if Pools (I).Pool = Name then
               return I;
            end if;
         end loop;
         Pools.Append ((Pool => Name, others => <>));
         return Pools.Last_Index;
      end Pool_Slot;

      procedure Check_Pool (T : Pool_Total; Label_Text : String) is
      begin
         if T.Need_Cpu > T.Free_Cpu or else T.Need_Mem > T.Free_Mem
           or else T.Need_Slots > T.Free_Slots
         then
            Add_Finding
              (Error, "CAPACITY_SHORTFALL", Label_Text,
               "pods leaving drained nodes need cpu " & Img (T.Need_Cpu)
               & "m, memory " & Img (T.Need_Mem / 2 ** 20) & "Mi, "
               & Img (T.Need_Slots) & " pod slots but kept schedulable nodes have "
               & Img (T.Free_Cpu) & "m, " & Img (T.Free_Mem / 2 ** 20) & "Mi, "
               & Img (T.Free_Slots) & " slots free");
         end if;
      end Check_Pool;
   begin
      if Natural (Pods.Length) > 0 and then not (for some N of Nodes => N.Role = Target) then
         Add_Finding (Warning, "NO_TARGETS", "snapshot",
                      "no node is marked drain, so there is nothing to plan");
      end if;
      for Q in Budgets.First_Index .. Budgets.Last_Index loop
         declare
            Level : constant Severity :=
              (if Budgets (Q).Touches_Target then Error else Warning);
            Name  : constant String := Str (Budgets (Q).Name);
         begin
            if Budgets (Q).Members = 0 then
               Add_Finding (Warning, "PDB_SELECTS_NOTHING", Name,
                            "selector matches no pod in namespace "
                            & Str (Budgets (Q).Namespace));
            elsif Budget_Error (Q) then
               Add_Finding
                 (Level, "PDB_UNCOMPUTABLE", Name,
                  "percentage or maxUnavailable budget covers "
                  & NImg (Budgets (Q).Naked)
                  & " pod(s) with no scalable controller, so the disruption "
                  & "controller cannot compute it and refuses every healthy eviction");
            elsif not Ever_Allows (Q) then
               Add_Finding
                 (Level, "ZERO_BUDGET", Name,
                  "never allows a disruption even when every pod is healthy ("
                  & "desired " & Img (Desired (Q)) & " of "
                  & NImg (Budgets (Q).Members) & " pods)");
            elsif Allowed (Q) = 0 then
               Add_Finding
                 (Warning, "PDB_DEGRADED", Name,
                  "allows 0 disruptions right now because only "
                  & NImg (Budgets (Q).Healthy) & " of "
                  & NImg (Budgets (Q).Members) & " pods are ready ("
                  & Budget_Numbers (Q) & ")");
            end if;
         end;
      end loop;

      for W in Workloads.First_Index .. Workloads.Last_Index loop
         if Amount (Workloads (W).Pod_Count) /= Workloads (W).Replicas then
            Add_Finding
              (Warning, "REPLICA_DRIFT", Str (Workloads (W).Name),
               "spec says " & Img (Workloads (W).Replicas) & " replicas but the "
               & "snapshot holds " & NImg (Workloads (W).Pod_Count)
               & " pods, so a rollout or scale event was in flight");
         end if;
      end loop;

      for P of Sorted_Pods loop
         declare
            On_Target : constant Boolean := Nodes (Pods (P).Node).Role = Target;
            Name      : constant String := Str (Pods (P).Name);
         begin
            if Pods (P).Covering.Length > 1 then
               declare
                  Names : Unbounded_String;
               begin
                  for Q of Pods (P).Covering loop
                     if Length (Names) > 0 then
                        Append (Names, ", ");
                     end if;
                     Append (Names, Budgets (Q).Name);
                  end loop;
                  Add_Finding
                    ((if On_Target then Error else Warning), "POD_MULTIPLE_PDBS", Name,
                     "matched by " & Str (Names)
                     & "; the eviction API rejects pods covered by more than one budget");
               end;
            end if;
            if On_Target and then not Pods (P).Daemon and then not Pods (P).Mirror then
               if Pods (P).Workload = 0 then
                  if Settings.Force then
                     Add_Finding (Warning, "UNMANAGED_POD_DELETED", Name,
                                  "has no controller and will be deleted, not recreated");
                  else
                     Add_Finding (Error, "UNMANAGED_POD", Name,
                                  "has no controller; drain refuses it without --force");
                  end if;
               end if;
               if Pods (P).Empty_Dir then
                  if Settings.Delete_Empty_Dir then
                     Add_Finding (Warning, "EMPTYDIR_DATA_LOSS", Name,
                                  "emptyDir data on the node is lost on eviction");
                  else
                     Add_Finding (Error, "LOCAL_STORAGE", Name,
                                  "uses emptyDir; drain refuses it without --delete-emptydir-data");
                  end if;
               end if;
               if Pods (P).Workload /= 0
                 and then Workloads (Pods (P).Workload).Replicas <= 1
                 and then Pods (P).Covering.Is_Empty
                 and then not Warned_Single.Contains (Pods (P).Workload)
               then
                  Warned_Single.Append (Pods (P).Workload);
                  Add_Finding (Warning, "SINGLE_REPLICA_OUTAGE",
                               Str (Workloads (Pods (P).Workload).Name),
                               "one replica and no budget: the drain takes it fully "
                               & "offline until the replacement is ready");
               end if;
            end if;
         end;
      end loop;

      --  Necessary capacity condition: everything that leaves the drained
      --  nodes must fit, in aggregate, on the nodes that stay. Bin packing can
      --  still fail when this passes; the simulation catches that case.
      for N in Nodes.First_Index .. Nodes.Last_Index loop
         if Nodes (N).Role = Keep and then not Nodes (N).Cordoned then
            declare
               I : constant Positive := Pool_Slot (Nodes (N).Pool);
               Cpu  : constant Amount := Amount'Max (0, Nodes (N).Cpu - Nodes (N).Used_Cpu);
               Mem  : constant Amount := Amount'Max (0, Nodes (N).Mem - Nodes (N).Used_Mem);
               Slot : constant Amount := Amount'Max (0, Nodes (N).Slots - Nodes (N).Used_Slots);
            begin
               Pools (I).Free_Cpu := Pools (I).Free_Cpu + Cpu;
               Pools (I).Free_Mem := Pools (I).Free_Mem + Mem;
               Pools (I).Free_Slots := Pools (I).Free_Slots + Slot;
               All_Pools.Free_Cpu := All_Pools.Free_Cpu + Cpu;
               All_Pools.Free_Mem := All_Pools.Free_Mem + Mem;
               All_Pools.Free_Slots := All_Pools.Free_Slots + Slot;
            end;
         end if;
      end loop;
      for P in Pods.First_Index .. Pods.Last_Index loop
         if Nodes (Pods (P).Node).Role = Target
           and then not Pods (P).Daemon and then not Pods (P).Mirror
           and then Pods (P).Workload /= 0
         then
            if Length (Pods (P).Pool) > 0 then
               declare
                  I : constant Positive := Pool_Slot (Pods (P).Pool);
               begin
                  Pools (I).Need_Cpu := Pools (I).Need_Cpu + Pods (P).Cpu;
                  Pools (I).Need_Mem := Pools (I).Need_Mem + Pods (P).Mem;
                  Pools (I).Need_Slots := Pools (I).Need_Slots + 1;
               end;
            end if;
            All_Pools.Need_Cpu := All_Pools.Need_Cpu + Pods (P).Cpu;
            All_Pools.Need_Mem := All_Pools.Need_Mem + Pods (P).Mem;
            All_Pools.Need_Slots := All_Pools.Need_Slots + 1;
         end if;
      end loop;
      for T of Pools loop
         Check_Pool (T, "pool " & Str (T.Pool));
      end loop;
      Check_Pool (All_Pools, "all pools");
   end Lint;

   ---------------------------------------------------------------------------
   --  Simulation
   ---------------------------------------------------------------------------

   function Count_Draining return Natural is
      Count : Natural := 0;
   begin
      for N of Nodes loop
         if N.State = Draining then
            Count := Count + 1;
         end if;
      end loop;
      return Count;
   end Count_Draining;

   function Blocked_On (N : Positive) return Natural is
      Count : Natural := 0;
   begin
      for P in Pods.First_Index .. Pods.Last_Index loop
         if Pods (P).Node = N and then Judge (P) not in Evictable | Skipped then
            Count := Count + 1;
         end if;
      end loop;
      return Count;
   end Blocked_On;

   procedure Start_Drains (Step : Natural; Next_Order : in out Natural;
                           Event : in out Boolean) is
   begin
      while Count_Draining < Settings.Max_Parallel loop
         declare
            Best         : Natural := 0;
            Best_Blocked : Natural := Natural'Last;
         begin
            for N in Nodes.First_Index .. Nodes.Last_Index loop
               if Nodes (N).Role = Target and then Nodes (N).State = Active then
                  declare
                     B : constant Natural := Blocked_On (N);
                  begin
                     if Best = 0
                       or else B < Best_Blocked
                       or else (B = Best_Blocked
                                and then (Nodes (N).Movable < Nodes (Best).Movable
                                          or else (Nodes (N).Movable = Nodes (Best).Movable
                                                   and then Nodes (N).Name < Nodes (Best).Name)))
                     then
                        Best := N;
                        Best_Blocked := B;
                     end if;
                  end;
               end if;
            end loop;
            exit when Best = 0;
            Next_Order := Next_Order + 1;
            Nodes (Best).State := Draining;
            Nodes (Best).Cordoned := True;
            Nodes (Best).Start_Step := Step;
            Nodes (Best).Order := Next_Order;
            Event := True;
         end;
      end loop;
   end Start_Drains;

   procedure Simulate is
      Step       : Natural := 0;
      Next_Order : Natural := 0;
      Event      : Boolean;
      Future     : Boolean;
      Unstarted  : Boolean;
   begin
      if Settings.Cordon_All then
         for N in Nodes.First_Index .. Nodes.Last_Index loop
            if Nodes (N).Role = Target then
               Nodes (N).Cordoned := True;
            end if;
         end loop;
      end if;

      loop
         Event := False;

         for P in Pods.First_Index .. Pods.Last_Index loop
            if Pods (P).Status = Starting and then Pods (P).Ready_At <= Step then
               Set_Status (P, Ready);
               Pods (P).Ready_At := Never;
               Event := True;
            end if;
         end loop;

         Start_Drains (Step, Next_Order, Event);

         for Rank in 1 .. Next_Order loop
            for N in Nodes.First_Index .. Nodes.Last_Index loop
               if Nodes (N).Order = Rank and then Nodes (N).State = Draining then
                  for P of Sorted_Pods loop
                     if Pods (P).Node = N and then Judge (P) = Evictable then
                        Evict (P);
                        Event := True;
                     end if;
                  end loop;
               end if;
            end loop;
         end loop;

         declare
            Still : Index_Vectors.Vector;
         begin
            for P of Pending_Queue loop
               if Place (P, Step) then
                  Event := True;
               else
                  Still.Append (P);
               end if;
            end loop;
            Pending_Queue := Still;
         end;

         for N in Nodes.First_Index .. Nodes.Last_Index loop
            if Nodes (N).State = Draining and then Nodes (N).Movable = 0 then
               Nodes (N).State := Drained;
               Nodes (N).End_Step := Step;
               for P in Pods.First_Index .. Pods.Last_Index loop
                  if Pods (P).Node = N then
                     Release (P);
                     Set_Status (P, Gone);
                  end if;
               end loop;
               Event := True;
            end if;
         end loop;

         if Event then
            Settled_Step := Step;
         end if;

         Future := False;
         for P of Pods loop
            if P.Status = Starting and then P.Ready_At /= Never then
               Future := True;
               exit;
            end if;
         end loop;

         Unstarted := False;
         for N of Nodes loop
            if N.Role = Target and then N.State = Active then
               Unstarted := True;
               exit;
            end if;
         end loop;

         if not Event and then not Future then
            --  Nothing can change any more. Park the stalled nodes so the
            --  remaining targets still get simulated and every blocker in the
            --  cluster is reported in one run.
            exit when Count_Draining = 0 and then not Unstarted;
            for N in Nodes.First_Index .. Nodes.Last_Index loop
               if Nodes (N).State = Draining then
                  Nodes (N).State := Stuck;
                  Nodes (N).End_Step := Step;
               end if;
            end loop;
         end if;

         Final_Step := Step;
         if Step >= Settings.Max_Steps then
            Timed_Out := True;
            exit;
         end if;
         Step := Step + 1;
      end loop;
      Final_Step := Step;
   end Simulate;

   ---------------------------------------------------------------------------
   --  Blocker attribution
   ---------------------------------------------------------------------------

   function Explain_Budget (Q : Positive) return String is
      Pending_Count, Broken_Count, Starting_Count : Natural := 0;
   begin
      for P in Pods.First_Index .. Pods.Last_Index loop
         if Pods (P).Covering.Contains (Q) then
            case Pods (P).Status is
               when Pending =>
                  Pending_Count := Pending_Count + 1;
               when Starting =>
                  if Pods (P).Ready_At = Never then
                     Broken_Count := Broken_Count + 1;
                  else
                     Starting_Count := Starting_Count + 1;
                  end if;
               when others =>
                  null;
            end case;
         end if;
      end loop;
      declare
         Base : constant String :=
           "pdb " & Str (Budgets (Q).Name) & " allows 0 disruptions ("
           & Budget_Numbers (Q) & ")";
      begin
         if not Ever_Allows (Q) then
            return Base & ": the budget is never satisfiable, raise replicas or relax it";
         elsif Pending_Count > 0 then
            return Base & ": " & NImg (Pending_Count)
              & " evicted pod(s) are Pending with no node that fits them";
         elsif Broken_Count > 0 then
            if Budgets (Q).Policy = Always_Allow then
               return Base & ": " & NImg (Broken_Count)
                 & " covered pod(s) are unready and never recover, so the budget has "
                 & "nothing left to spend; fix or remove them";
            end if;
            return Base & ": " & NImg (Broken_Count)
              & " covered pod(s) are unready and never recover; fix them or set "
              & "unhealthyPodEvictionPolicy AlwaysAllow so they can be evicted first";
         elsif Starting_Count > 0 then
            return Base & ": waiting on " & NImg (Starting_Count) & " pod(s) to become ready";
         else
            return Base;
         end if;
      end;
   end Explain_Budget;

   procedure Attribute is
   begin
      for N in Nodes.First_Index .. Nodes.Last_Index loop
         if Nodes (N).Role = Target and then Nodes (N).State /= Drained then
            for P of Sorted_Pods loop
               if Pods (P).Node = N then
                  declare
                     V : constant Verdict := Judge (P);
                     Code, Message : Unbounded_String;
                  begin
                     case V is
                        when Evictable | Skipped =>
                           null;
                        when Blocked_Unmanaged =>
                           Code := U ("UNMANAGED_POD");
                           Message := U ("no controller owns this pod; drain needs --force "
                                         & "and the pod will not come back");
                        when Blocked_Empty_Dir =>
                           Code := U ("LOCAL_STORAGE");
                           Message := U ("emptyDir volume; drain needs --delete-emptydir-data");
                        when Blocked_Multiple_Budgets =>
                           Code := U ("POD_MULTIPLE_PDBS");
                           Message := U ("covered by more than one pdb; the eviction API "
                                         & "returns an error, merge or narrow the selectors");
                        when Blocked_Budget_Error =>
                           Code := U ("PDB_UNCOMPUTABLE");
                           Message := U ("pdb " & Str (Budgets (Pods (P).Covering.First_Element).Name)
                                         & " covers pods with no scalable controller");
                        when Blocked_Budget =>
                           Code := U ("PDB_EXHAUSTED");
                           Message := U (Explain_Budget (Pods (P).Covering.First_Element));
                     end case;
                     if Length (Code) > 0 then
                        Blockers.Append ((Node => N, Pod => P, Code => Code,
                                          Message => Message));
                     end if;
                  end;
               end if;
            end loop;
         end if;
      end loop;
   end Attribute;

   ---------------------------------------------------------------------------
   --  Output
   ---------------------------------------------------------------------------

   function Duration_Text (Step : Natural) return String is
      Seconds : constant Amount := Amount (Step) * Settings.Tick_Seconds;
      H       : constant Amount := Seconds / 3600;
      M       : constant Amount := (Seconds mod 3600) / 60;
      Sec     : constant Amount := Seconds mod 60;
      function Two (X : Amount) return String is
        ((if X < 10 then "0" else "") & Img (X));
   begin
      if H > 0 then
         return Img (H) & "h" & Two (M) & "m" & Two (Sec) & "s";
      elsif M > 0 then
         return Img (M) & "m" & Two (Sec) & "s";
      else
         return Img (Sec) & "s";
      end if;
   end Duration_Text;

   function Json_Escape (Text : String) return String is
      Result : Unbounded_String;
      Hex    : constant String := "0123456789abcdef";
   begin
      for C of Text loop
         case C is
            when '"' =>
               Append (Result, "\""");
            when '\' =>
               Append (Result, "\\");
            when ASCII.LF =>
               Append (Result, "\n");
            when ASCII.HT =>
               Append (Result, "\t");
            when ASCII.CR =>
               Append (Result, "\r");
            when ASCII.NUL .. ASCII.BS | ASCII.VT | ASCII.FF | ASCII.SO .. ASCII.US =>
               Append (Result, "\u00" & Hex (Character'Pos (C) / 16 + 1)
                       & Hex (Character'Pos (C) mod 16 + 1));
            when others =>
               Append (Result, C);
         end case;
      end loop;
      return Str (Result);
   end Json_Escape;

   function Q (Text : String) return String is ("""" & Json_Escape (Text) & """");

   function Pad (Text : String; Width : Positive) return String is
     (if Text'Length >= Width then Text & " "
      else Text & (1 .. Width - Text'Length => ' '));

   function Target_Count return Natural is
      Count : Natural := 0;
   begin
      for N of Nodes loop
         if N.Role = Target then
            Count := Count + 1;
         end if;
      end loop;
      return Count;
   end Target_Count;

   function Drained_Count return Natural is
      Count : Natural := 0;
   begin
      for N of Nodes loop
         if N.Role = Target and then N.State = Drained then
            Count := Count + 1;
         end if;
      end loop;
      return Count;
   end Drained_Count;

   function Drain_Done_Step return Natural is
      Last : Natural := 0;
   begin
      for N of Nodes loop
         if N.State = Drained then
            Last := Natural'Max (Last, N.End_Step);
         end if;
      end loop;
      return Last;
   end Drain_Done_Step;

   function Error_Count return Natural is
      Count : Natural := 0;
   begin
      for F of Findings loop
         if F.Level = Error then
            Count := Count + 1;
         end if;
      end loop;
      return Count;
   end Error_Count;

   function Plan_Order return Index_Vectors.Vector is
      Result : Index_Vectors.Vector;
      function Less (A, B : Positive) return Boolean is
        (if Nodes (A).Order /= Nodes (B).Order then
           (Nodes (A).Order /= 0 and then (Nodes (B).Order = 0 or else Nodes (A).Order < Nodes (B).Order))
         else Nodes (A).Name < Nodes (B).Name);
      package Sorting is new Index_Vectors.Generic_Sorting ("<" => Less);
   begin
      for N in Nodes.First_Index .. Nodes.Last_Index loop
         if Nodes (N).Role = Target then
            Result.Append (N);
         end if;
      end loop;
      Sorting.Sort (Result);
      return Result;
   end Plan_Order;

   function State_Text (N : Positive) return String is
     (case Nodes (N).State is
         when Drained  => "drained",
         when Stuck    => "stuck",
         when Draining => "draining",
         when Active   => "not started");

   function Result_Text return String is
     (if Timed_Out then "TIMEOUT"
      elsif Drained_Count = Target_Count and then Pending_Queue.Is_Empty
        and then Blockers.Is_Empty then "COMPLETE"
      else "BLOCKED");

   procedure Report_Text is
      Order : constant Index_Vectors.Vector := Plan_Order;
   begin
      Put_Line ("PDB Drain Planner");
      Put_Line ("snapshot: " & NImg (Natural (Nodes.Length)) & " nodes ("
                & NImg (Target_Count) & " to drain), "
                & NImg (Natural (Pods.Length)) & " pods, "
                & NImg (Natural (Budgets.Length)) & " pdbs, "
                & NImg (Natural (Workloads.Length)) & " workloads");
      Put_Line ("settings: tick " & Img (Settings.Tick_Seconds) & "s, max parallel "
                & NImg (Settings.Max_Parallel)
                & ", cordon all " & (if Settings.Cordon_All then "yes" else "no")
                & ", force " & (if Settings.Force then "yes" else "no")
                & ", delete emptydir data "
                & (if Settings.Delete_Empty_Dir then "yes" else "no"));
      New_Line;

      Put_Line ("Findings");
      if Findings.Is_Empty then
         Put_Line ("  none");
      end if;
      for F of Findings loop
         Put_Line ("  " & Pad ((if F.Level = Error then "ERROR" else "WARN"), 7)
                   & Pad (Str (F.Code), 22) & Str (F.Subject) & ": " & Str (F.Message));
      end loop;
      New_Line;

      Put_Line ("Drain plan");
      Put_Line ("  " & Pad ("#", 4) & Pad ("node", 32) & Pad ("start", 10)
                & Pad ("end", 10) & Pad ("evicted", 9) & Pad ("landed", 8) & "state");
      for N of Order loop
         Put_Line
           ("  " & Pad ((if Nodes (N).Order = 0 then "-" else NImg (Nodes (N).Order)), 4)
            & Pad (Str (Nodes (N).Name), 32)
            & Pad ((if Nodes (N).Start_Step = Never then "-"
                    else Duration_Text (Nodes (N).Start_Step)), 10)
            & Pad ((if Nodes (N).End_Step = Never then "-"
                    else Duration_Text (Nodes (N).End_Step)), 10)
            & Pad (NImg (Nodes (N).Evicted), 9)
            & Pad (NImg (Nodes (N).Landed), 8)
            & State_Text (N));
      end loop;
      New_Line;

      Put_Line ("Blockers");
      if Blockers.Is_Empty and then Pending_Queue.Is_Empty then
         Put_Line ("  none");
      end if;
      for B of Blockers loop
         Put_Line ("  " & Str (Nodes (B.Node).Name) & "  " & Str (Pods (B.Pod).Name)
                   & "  " & Str (B.Code) & ": " & Str (B.Message));
      end loop;
      for P of Pending_Queue loop
         Put_Line ("  (unscheduled)  " & Str (Pods (P).Name)
                   & "  NO_CAPACITY: needs cpu " & Img (Pods (P).Cpu) & "m, memory "
                   & Img (Pods (P).Mem / 2 ** 20) & "Mi"
                   & (if Length (Pods (P).Pool) > 0
                      then " in pool " & Str (Pods (P).Pool) else "")
                   & " and no schedulable node fits");
      end loop;
      New_Line;

      Put_Line ("Budgets touched");
      for Q in Budgets.First_Index .. Budgets.Last_Index loop
         if Budgets (Q).Touches_Target then
            Put_Line ("  " & Pad (Str (Budgets (Q).Name), 36)
                      & "lowest healthy " & NImg (Budgets (Q).Min_Healthy)
                      & ", most down at once " & NImg (Budgets (Q).Max_Down)
                      & ", members now " & NImg (Budgets (Q).Members));
         end if;
      end loop;
      New_Line;

      Put_Line ("Summary");
      Put_Line ("  result: " & Result_Text
                & " | drained " & NImg (Drained_Count) & "/" & NImg (Target_Count)
                & " | evictions " & NImg (Total_Evictions)
                & " (repeat " & NImg (Repeat_Evictions) & ")"
                & " | drain done at " & (if Drained_Count = 0 then "-" else Duration_Text (Drain_Done_Step))
                & " | settled at " & Duration_Text (Settled_Step)
                & " | ticks " & NImg (Final_Step));
      if Repeat_Evictions > 0 and then not Settings.Cordon_All then
         Put_Line ("  hint: " & NImg (Repeat_Evictions)
                   & " evictions hit pods that had already moved once; "
                   & "--cordon-all keeps replacements off nodes that are about to drain");
      end if;
   end Report_Text;

   procedure Report_Json is
      Order : constant Index_Vectors.Vector := Plan_Order;
      First : Boolean;
   begin
      Put_Line ("{");
      Put_Line ("  ""result"": " & Q (Result_Text) & ",");
      Put_Line ("  ""tick_seconds"": " & Img (Settings.Tick_Seconds) & ",");
      Put_Line ("  ""targets"": " & NImg (Target_Count) & ",");
      Put_Line ("  ""drained"": " & NImg (Drained_Count) & ",");
      Put_Line ("  ""evictions"": " & NImg (Total_Evictions) & ",");
      Put_Line ("  ""repeat_evictions"": " & NImg (Repeat_Evictions) & ",");
      Put_Line ("  ""drain_done_seconds"": "
                & Img (Amount (Drain_Done_Step) * Settings.Tick_Seconds) & ",");
      Put_Line ("  ""settled_seconds"": "
                & Img (Amount (Settled_Step) * Settings.Tick_Seconds) & ",");
      Put_Line ("  ""ticks"": " & NImg (Final_Step) & ",");
      Put_Line ("  ""findings"": [");
      First := True;
      for F of Findings loop
         Put_Line ((if First then "" else ",") & "    {""level"": "
                   & Q ((if F.Level = Error then "error" else "warning"))
                   & ", ""code"": " & Q (Str (F.Code))
                   & ", ""subject"": " & Q (Str (F.Subject))
                   & ", ""message"": " & Q (Str (F.Message)) & "}");
         First := False;
      end loop;
      Put_Line ("  ],");
      Put_Line ("  ""plan"": [");
      First := True;
      for N of Order loop
         Put_Line ((if First then "" else ",") & "    {""node"": " & Q (Str (Nodes (N).Name))
                   & ", ""order"": " & NImg (Nodes (N).Order)
                   & ", ""state"": " & Q (State_Text (N))
                   & ", ""start_seconds"": "
                   & (if Nodes (N).Start_Step = Never then "null"
                      else Img (Amount (Nodes (N).Start_Step) * Settings.Tick_Seconds))
                   & ", ""end_seconds"": "
                   & (if Nodes (N).End_Step = Never then "null"
                      else Img (Amount (Nodes (N).End_Step) * Settings.Tick_Seconds))
                   & ", ""evicted"": " & NImg (Nodes (N).Evicted)
                   & ", ""landed"": " & NImg (Nodes (N).Landed) & "}");
         First := False;
      end loop;
      Put_Line ("  ],");
      Put_Line ("  ""blockers"": [");
      First := True;
      for B of Blockers loop
         Put_Line ((if First then "" else ",") & "    {""node"": " & Q (Str (Nodes (B.Node).Name))
                   & ", ""pod"": " & Q (Str (Pods (B.Pod).Name))
                   & ", ""code"": " & Q (Str (B.Code))
                   & ", ""message"": " & Q (Str (B.Message)) & "}");
         First := False;
      end loop;
      for P of Pending_Queue loop
         Put_Line ((if First then "" else ",") & "    {""node"": null, ""pod"": "
                   & Q (Str (Pods (P).Name))
                   & ", ""code"": ""NO_CAPACITY"", ""message"": "
                   & Q ("no schedulable node fits cpu " & Img (Pods (P).Cpu)
                        & "m, memory " & Img (Pods (P).Mem / 2 ** 20) & "Mi") & "}");
         First := False;
      end loop;
      Put_Line ("  ],");
      Put_Line ("  ""budgets"": [");
      First := True;
      for B in Budgets.First_Index .. Budgets.Last_Index loop
         if Budgets (B).Touches_Target then
            Put_Line ((if First then "" else ",") & "    {""pdb"": " & Q (Str (Budgets (B).Name))
                      & ", ""lowest_healthy"": " & NImg (Budgets (B).Min_Healthy)
                      & ", ""most_down"": " & NImg (Budgets (B).Max_Down) & "}");
            First := False;
         end if;
      end loop;
      Put_Line ("  ]");
      Put_Line ("}");
   end Report_Json;

   ---------------------------------------------------------------------------
   --  Command line
   ---------------------------------------------------------------------------

   procedure Print_Help is
   begin
      Put_Line ("usage: pdbdrainplanner [options] [SNAPSHOT|-]");
      Put_Line ("");
      Put_Line ("Simulates draining every node marked drain under PodDisruptionBudget");
      Put_Line ("eviction rules and reports the order, timing and every blocker.");
      Put_Line ("");
      Put_Line ("options:");
      Put_Line ("  --tick-seconds N        seconds per simulated eviction retry (default 5)");
      Put_Line ("  --max-parallel N        nodes drained at the same time (default 1)");
      Put_Line ("  --max-steps N           safety limit on simulated ticks (default 100000)");
      Put_Line ("  --cordon-all            cordon every drain target before the first eviction");
      Put_Line ("  --force                 allow pods without a controller (they are deleted)");
      Put_Line ("  --delete-emptydir-data  allow pods with emptyDir volumes");
      Put_Line ("  --json                  machine readable output");
      Put_Line ("  --strict                exit 1 on any warning as well");
      Put_Line ("");
      Put_Line ("snapshot records, one per line, # starts a comment:");
      Put_Line ("  node NAME POOL CPU MEMORY PODS keep|drain schedulable|cordoned");
      Put_Line ("  workload NAMESPACE/NAME REPLICAS READY_SECONDS");
      Put_Line ("  pdb NAMESPACE/NAME minAvailable|maxUnavailable N|N% "
                & "IfHealthyBudget|AlwaysAllow SELECTOR");
      Put_Line ("  pod NAMESPACE/NAME NODE WORKLOAD|- CPU MEMORY ready|unready "
                & "POOL|- LABELS|- FLAGS|-");
      Put_Line ("");
      Put_Line ("exit codes: 0 every target drains, 1 blocked or error findings, 2 bad input");
   end Print_Help;

   procedure Parse_Arguments is
      use Ada.Command_Line;
      I         : Positive := 1;
      Have_Path : Boolean := False;

      function Next_Value (Flag : String; Low, High : Amount) return Amount is
      begin
         if I >= Argument_Count then
            raise Usage_Error with Flag & " needs a value";
         end if;
         I := I + 1;
         declare
            V : Amount;
         begin
            V := Parse_Natural (Argument (I), 0, Flag);
            if V < Low or else V > High then
               raise Usage_Error with Flag & " must be between " & Img (Low)
                 & " and " & Img (High);
            end if;
            return V;
         exception
            when Input_Error =>
               raise Usage_Error with Flag & " needs a whole number";
         end;
      end Next_Value;
   begin
      while I <= Argument_Count loop
         declare
            A : constant String := Argument (I);
         begin
            if A = "-h" or else A = "--help" then
               Print_Help;
               Set_Exit_Status (Exit_Status (Exit_Complete));
               raise Usage_Error with "";
            elsif A = "--tick-seconds" then
               Settings.Tick_Seconds := Next_Value (A, 1, 3600);
            elsif A = "--max-parallel" then
               Settings.Max_Parallel := Natural (Next_Value (A, 1, Max_Parallel_Cap));
            elsif A = "--max-steps" then
               Settings.Max_Steps := Natural (Next_Value (A, 1, Max_Steps_Cap));
            elsif A = "--cordon-all" then
               Settings.Cordon_All := True;
            elsif A = "--force" then
               Settings.Force := True;
            elsif A = "--delete-emptydir-data" then
               Settings.Delete_Empty_Dir := True;
            elsif A = "--json" then
               Settings.Json := True;
            elsif A = "--strict" then
               Settings.Strict := True;
            elsif A'Length > 1 and then A (A'First) = '-' then
               raise Usage_Error with "unknown option " & A;
            elsif Have_Path then
               raise Usage_Error with "only one snapshot path is accepted";
            else
               Settings.Path := U (A);
               Have_Path := True;
            end if;
         end;
         I := I + 1;
      end loop;
   end Parse_Arguments;

begin
   Parse_Arguments;
   Load;
   Resolve;
   Lint;
   Simulate;
   Attribute;
   if Settings.Json then
      Report_Json;
   else
      Report_Text;
   end if;
   if Result_Text /= "COMPLETE"
     or else Error_Count > 0
     or else (Settings.Strict and then not Findings.Is_Empty)
   then
      Ada.Command_Line.Set_Exit_Status (Ada.Command_Line.Exit_Status (Exit_Blocked));
   else
      Ada.Command_Line.Set_Exit_Status (Ada.Command_Line.Exit_Status (Exit_Complete));
   end if;
exception
   when E : Usage_Error =>
      if Ada.Exceptions.Exception_Message (E) /= "" then
         Put_Line (Standard_Error, "pdbdrainplanner: "
                   & Ada.Exceptions.Exception_Message (E));
         Put_Line (Standard_Error, "run with --help for the grammar");
         Ada.Command_Line.Set_Exit_Status (Ada.Command_Line.Exit_Status (Exit_Usage));
      end if;
   when E : Input_Error =>
      Put_Line (Standard_Error, "pdbdrainplanner: " & Ada.Exceptions.Exception_Message (E));
      Ada.Command_Line.Set_Exit_Status (Ada.Command_Line.Exit_Status (Exit_Usage));
end PdbDrainPlanner;
