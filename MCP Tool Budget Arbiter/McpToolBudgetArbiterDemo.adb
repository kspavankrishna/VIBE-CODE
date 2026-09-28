with Ada.Text_IO;             use Ada.Text_IO;
with Ada.Real_Time;           use Ada.Real_Time;
with Ada.Numerics.Discrete_Random;
with MCP_Tool_Budget_Arbiter; use MCP_Tool_Budget_Arbiter;

--  Simulates four concurrent MCP agent sessions hammering one shared cost
--  budget through the Arbiter, plus a background reaper task that
--  reclaims whatever a "crashed" session leaves behind. Everything here
--  runs through the public Reserve / Commit / Rollback / Expire_Stale
--  operations declared in McpToolBudgetArbiter.ads; nothing pokes at the
--  Arbiter's private state.
procedure McpToolBudgetArbiterDemo is

   Governor : Arbiter (Capacity => 30_000, Lease_Milliseconds => 150);

   subtype Cost_Roll is Budget_Units range 50 .. 900;
   package Cost_Random is new Ada.Numerics.Discrete_Random (Cost_Roll);

   task type Agent_Session (Name : Character; Calls : Positive);

   task body Agent_Session is
      Gen    : Cost_Random.Generator;
      Tenant : constant Tenant_Id := To_Tenant_Id ("agent-" & Name);
   begin
      Cost_Random.Reset (Gen);

      for Call in 1 .. Calls loop
         declare
            Ask    : constant Budget_Units := Cost_Random.Random (Gen);
            Result : Reservation_Result;
         begin
            Governor.Reserve (Tenant, Ask, Result);

            case Result.Status is
               when Granted =>
                  --  simulate the tool call itself taking some real time,
                  --  during which the reservation holds its slice of the
                  --  shared budget
                  delay 0.01;

                  if Call mod 7 = 0 then
                     --  simulate a session that vanishes mid call: no
                     --  Commit, no Rollback, the reaper must reclaim it
                     null;
                  elsif Call mod 5 = 0 then
                     declare
                        Ok : Boolean;
                     begin
                        Governor.Rollback (Result.Id, Ok);
                     end;
                  else
                     declare
                        Ok          : Boolean;
                        --  the real cost of an LLM or tool call is usually
                        --  lower than the worst case estimate it was
                        --  reserved against; Commit settles at the true
                        --  cost and the difference flows back to Balance
                        Actual_Cost : constant Budget_Units := Ask - Ask / 4;
                     begin
                        Governor.Commit (Result.Id, Actual_Cost, Ok);
                     end;
                  end if;

               when Rejected_Insufficient_Budget =>
                  Put_Line
                    ("session " & Name & " backed off: budget exhausted");
                  delay 0.02;

               when others =>
                  null;
            end case;
         end;
      end loop;
   end Agent_Session;

begin
   Put_Line ("MCP Tool Budget Arbiter demo");
   Put_Line ("initial capacity:" & Budget_Units'Image (30_000));
   Put_Line ("");

   declare
      A : Agent_Session (Name => 'A', Calls => 40);
      B : Agent_Session (Name => 'B', Calls => 40);
      C : Agent_Session (Name => 'C', Calls => 40);
      D : Agent_Session (Name => 'D', Calls => 40);

      task Reaper;

      task body Reaper is
         Deadline : Time := Clock;
      begin
         for Sweep in 1 .. 150 loop
            Deadline := Deadline + Milliseconds (25);
            delay until Deadline;

            declare
               Reclaimed : Natural;
            begin
               Governor.Expire_Stale (Clock, Reclaimed);
               if Reclaimed > 0 then
                  Put_Line
                    ("reaper reclaimed " & Natural'Image (Reclaimed) &
                     " abandoned reservation(s)");
               end if;
            end;
         end loop;
      end Reaper;
   begin
      --  this block does not return until A, B, C, D and Reaper have all
      --  terminated: Ada awaits tasks at the end of their enclosing scope
      --  by construction, no manual join or wait group required
      null;
   end;

   Put_Line ("");
   Put_Line ("run complete");

   declare
      Snapshot : Ledger_Window (1 .. Max_Ledger_Entries);
      Count    : Natural;
   begin
      Governor.Copy_Ledger (Snapshot, Count);

      Put_Line ("ledger entries retained:  " & Natural'Image (Count));
      Put_Line
        ("outstanding reservations: " &
         Natural'Image (Governor.Outstanding_Count));
      Put_Line
        ("available budget:         " &
         Budget_Units'Image (Governor.Available));

      if Governor.Verify_Ledger_Integrity then
         Put_Line ("hash chain integrity:     OK");
      else
         Put_Line ("hash chain integrity:     FAILED");
      end if;

      if Count > 0 then
         Put_Line
           ("first retained event: sequence" &
            Natural'Image (Snapshot (1).Sequence) & " " &
            Ledger_Event_Kind'Image (Snapshot (1).Event));
         Put_Line
           ("last retained event:  sequence" &
            Natural'Image (Snapshot (Count).Sequence) & " " &
            Ledger_Event_Kind'Image (Snapshot (Count).Event));
      end if;
   end;
end McpToolBudgetArbiterDemo;
